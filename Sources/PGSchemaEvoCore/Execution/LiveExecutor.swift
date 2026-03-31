import Logging

/// Executes clone steps against a live target database by shelling out to psql.
///
/// All DDL is executed via `psql` rather than through PostgresNIO directly.
/// This approach ensures that the same SQL syntax works in both dry-run scripts
/// and live execution, and leverages psql's robust error handling and transaction
/// support. Data transfer uses either psql COPY pipes or pg_dump/pg_restore.
public struct LiveExecutor: Sendable {
    private let shell: ShellRunner
    private let logger: Logger

    public init(logger: Logger) {
        self.shell = ShellRunner()
        self.logger = logger
    }

    /// Execute all clone steps in a single psql session for true transaction isolation.
    ///
    /// This method:
    /// 1. Pre-fetches source data for all COPY steps (separate processes per source DB)
    /// 2. Builds a complete SQL script with BEGIN/COMMIT wrapping
    /// 3. Executes the entire script in one psql process against the target DB
    ///
    /// Because all SQL runs in a single psql session, BEGIN/COMMIT provide true
    /// transaction semantics — either everything succeeds or nothing is committed.
    /// On failure, PostgreSQL automatically rolls back when the session disconnects.
    public func executeInTransaction(steps: [CloneStep], job: CloneJob) async throws {
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql",
                exitCode: -1,
                stderr: "psql not found in PATH. Install postgresql-client."
            )
        }

        let targetDSN = job.target.toDSN()
        let sourceDSN = job.source.toDSN()
        let env = job.target.environment()
        let sourceEnv = job.source.environment()

        let progress = ProgressReporter(totalSteps: steps.count)
        progress.reportStart(objectCount: job.objects.count)

        // Phase 1: Pre-fetch source data for all COPY steps
        var prefetchedData: [Int: String] = [:]
        for (index, step) in steps.enumerated() {
            if case .copyData(let id, let method, _, let whereClause, let rowLimit) = step {
                let stepNum = index + 1
                progress.reportStep(stepNum, description: "Fetching data for \(id)")

                let data = try await fetchSourceData(
                    psqlPath: psqlPath,
                    sourceDSN: sourceDSN,
                    id: id,
                    method: method,
                    sourceEnv: sourceEnv,
                    whereClause: whereClause,
                    rowLimit: rowLimit
                )
                prefetchedData[index] = data

                progress.reportStepComplete(stepNum, description: "Fetched data for \(id)")
            }
        }

        // Phase 2: Build complete SQL script
        let script = buildTransactionScript(steps: steps, prefetchedData: prefetchedData)
        logger.debug("Transaction script (\(script.count) bytes, \(steps.count) steps)")

        // Phase 3: Execute the entire script in a single psql process
        progress.reportStep(steps.count, description: "Executing transaction")

        SignalHandler.shared.setTransactionContext(true)
        let result: ShellResult
        do {
            result = try await shell.run(
                command: psqlPath,
                arguments: [targetDSN, "--set", "ON_ERROR_STOP=1", "-X"],
                environment: env,
                input: script
            )
        } catch {
            SignalHandler.shared.setTransactionContext(false)
            throw error
        }
        SignalHandler.shared.setTransactionContext(false)

        guard result.succeeded else {
            // No explicit ROLLBACK needed — PostgreSQL automatically rolls back
            // uncommitted transactions when the session disconnects.
            logger.error("Transaction failed: \(result.stderr)")
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql (transaction)",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }

        logger.debug("Transaction output: \(result.stdout)")
        progress.reportComplete(stepCount: steps.count)
    }

    /// Execute all clone steps without transaction wrapping (legacy path).
    public func execute(steps: [CloneStep], job: CloneJob) async throws {
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql",
                exitCode: -1,
                stderr: "psql not found in PATH. Install postgresql-client."
            )
        }

        let targetDSN = job.target.toDSN()
        let sourceDSN = job.source.toDSN()
        let env = job.target.environment()
        let sourceEnv = job.source.environment()

        let progress = ProgressReporter(totalSteps: steps.count)
        progress.reportStart(objectCount: job.objects.count)

        for (index, step) in steps.enumerated() {
            let stepNum = index + 1
            let desc = stepDescription(step)
            progress.reportStep(stepNum, description: desc)

            do {
                try await executeStep(
                    step: step,
                    psqlPath: psqlPath,
                    targetDSN: targetDSN,
                    sourceDSN: sourceDSN,
                    env: env,
                    sourceEnv: sourceEnv
                )
                progress.reportStepComplete(stepNum, description: desc)
            } catch {
                progress.reportStepFailed(stepNum, description: desc, error: error.localizedDescription)
                throw error
            }
        }

        progress.reportComplete(stepCount: steps.count)
    }

    // MARK: - Transaction Script Building

    /// Build a complete SQL script that runs all steps within a single transaction.
    func buildTransactionScript(
        steps: [CloneStep],
        prefetchedData: [Int: String]
    ) -> String {
        var script = "BEGIN;\n\n"

        for (index, step) in steps.enumerated() {
            script += "-- Step \(index + 1): \(stepDescription(step))\n"

            switch step {
            case .dropObject(let id):
                script += generateDropSQL(for: id) + "\n\n"

            case .createObject(let sql, _):
                script += sql + "\n\n"

            case .alterObject(let sql, _):
                script += sql + "\n\n"

            case .copyData(let id, let method, _, _, _):
                if let data = prefetchedData[index], !data.isEmpty {
                    switch method {
                    case .pgDump:
                        // pg_dump --format=plain output is SQL-ready (contains COPY statements)
                        script += data
                        if !data.hasSuffix("\n") { script += "\n" }
                        script += "\n"
                    case .copy, .auto:
                        // Inline COPY FROM STDIN with CSV data, terminated by \.
                        script += "COPY \(id.qualifiedName) FROM STDIN WITH (FORMAT csv, HEADER);\n"
                        script += data
                        if !data.hasSuffix("\n") { script += "\n" }
                        script += "\\.\n\n"
                    }
                }

            case .grantPermissions(let sql, _):
                script += sql + "\n\n"

            case .refreshMaterializedView(let id):
                script += "REFRESH MATERIALIZED VIEW \(id.qualifiedName);\n\n"

            case .enableRLS(let sql, _):
                script += sql + "\n\n"

            case .attachPartition(let sql, _):
                script += sql + "\n\n"
            }
        }

        script += "COMMIT;\n"
        return script
    }

    /// Pre-fetch source data for a COPY step from the source database.
    private func fetchSourceData(
        psqlPath: String,
        sourceDSN: String,
        id: ObjectIdentifier,
        method: TransferMethod,
        sourceEnv: [String: String],
        whereClause: String?,
        rowLimit: Int?
    ) async throws -> String {
        switch method {
        case .copy, .auto:
            let copyCommand: String
            if whereClause != nil || rowLimit != nil {
                var query = "SELECT * FROM \(id.qualifiedName)"
                if let wh = whereClause { query += " WHERE \(wh)" }
                if let lim = rowLimit { query += " LIMIT \(lim)" }
                copyCommand = "\\copy (\(query)) TO STDOUT WITH (FORMAT csv, HEADER)"
            } else {
                copyCommand = "\\copy \(id.qualifiedName) TO STDOUT WITH (FORMAT csv, HEADER)"
            }

            let result = try await shell.run(
                command: psqlPath,
                arguments: [sourceDSN, "-c", copyCommand],
                environment: sourceEnv
            )

            guard result.succeeded else {
                throw PGSchemaEvoError.shellCommandFailed(
                    command: "psql COPY export \(id)",
                    exitCode: result.exitCode,
                    stderr: result.stderr
                )
            }

            logger.info("Fetched data for \(id) (\(result.stdout.count) bytes)")
            return result.stdout

        case .pgDump:
            guard let pgDumpPath = shell.which("pg_dump") else {
                throw PGSchemaEvoError.shellCommandFailed(
                    command: "pg_dump",
                    exitCode: -1,
                    stderr: "pg_dump not found in PATH"
                )
            }

            // Use --format=plain to get SQL output that can be inlined in the transaction
            let result = try await shell.run(
                command: pgDumpPath,
                arguments: [
                    "--format=plain", "--data-only",
                    "--table=\(id.qualifiedName)", sourceDSN,
                ],
                environment: sourceEnv
            )

            guard result.succeeded else {
                throw PGSchemaEvoError.shellCommandFailed(
                    command: "pg_dump \(id)",
                    exitCode: result.exitCode,
                    stderr: result.stderr
                )
            }

            logger.info("Fetched data via pg_dump for \(id) (\(result.stdout.count) bytes)")
            return result.stdout
        }
    }

    // MARK: - Per-Step Execution (used by non-transaction path)

    private func executeStep(
        step: CloneStep,
        psqlPath: String,
        targetDSN: String,
        sourceDSN: String,
        env: [String: String],
        sourceEnv: [String: String]
    ) async throws {
        switch step {
        case .dropObject(let id):
            let dropSQL = generateDropSQL(for: id)
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: dropSQL,
                env: env,
                description: "DROP \(id)"
            )

        case .createObject(let sql, let id):
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: sql,
                env: env,
                description: "CREATE \(id)"
            )

        case .alterObject(let sql, let id):
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: sql,
                env: env,
                description: "ALTER \(id)"
            )

        case .copyData(let id, let method, _, let whereClause, let rowLimit):
            switch method {
            case .copy, .auto:
                try await copyViaPsqlPipe(
                    psqlPath: psqlPath,
                    sourceDSN: sourceDSN,
                    targetDSN: targetDSN,
                    id: id,
                    sourceEnv: sourceEnv,
                    targetEnv: env,
                    whereClause: whereClause,
                    rowLimit: rowLimit
                )
            case .pgDump:
                try await copyViaPgDump(
                    sourceDSN: sourceDSN,
                    targetDSN: targetDSN,
                    id: id,
                    sourceEnv: sourceEnv
                )
            }

        case .grantPermissions(let sql, _):
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: sql,
                env: env,
                description: "GRANT permissions"
            )

        case .refreshMaterializedView(let id):
            let sql = "REFRESH MATERIALIZED VIEW \(id.qualifiedName);"
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: sql,
                env: env,
                description: "REFRESH \(id)"
            )

        case .enableRLS(let sql, let id):
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: sql,
                env: env,
                description: "ENABLE RLS \(id)"
            )

        case .attachPartition(let sql, let id):
            try await executePsql(
                psqlPath: psqlPath,
                dsn: targetDSN,
                sql: sql,
                env: env,
                description: "ATTACH PARTITION \(id)"
            )
        }
    }

    private func executePsql(
        psqlPath: String,
        dsn: String,
        sql: String,
        env: [String: String],
        description: String
    ) async throws {
        let result = try await shell.run(
            command: psqlPath,
            arguments: [dsn, "--set", "ON_ERROR_STOP=1"],
            environment: env,
            input: sql
        )

        guard result.succeeded else {
            logger.error("\(description) failed: \(result.stderr)")
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql (\(description))",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }

        logger.debug("\(description): \(result.stdout)")
    }

    private func copyViaPsqlPipe(
        psqlPath: String,
        sourceDSN: String,
        targetDSN: String,
        id: ObjectIdentifier,
        sourceEnv: [String: String],
        targetEnv: [String: String],
        whereClause: String? = nil,
        rowLimit: Int? = nil
    ) async throws {
        let copySource: String
        if whereClause != nil || rowLimit != nil {
            var query = "SELECT * FROM \(id.qualifiedName)"
            if let wh = whereClause { query += " WHERE \(wh)" }
            if let lim = rowLimit { query += " LIMIT \(lim)" }
            copySource = "\\copy (\(query)) TO STDOUT WITH (FORMAT csv, HEADER)"
        } else {
            copySource = "\\copy \(id.qualifiedName) TO STDOUT WITH (FORMAT csv, HEADER)"
        }

        // Export from source
        let exportResult = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", copySource],
            environment: sourceEnv
        )

        guard exportResult.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql COPY export \(id)",
                exitCode: exportResult.exitCode,
                stderr: exportResult.stderr
            )
        }

        // Import to target
        let importResult = try await shell.run(
            command: psqlPath,
            arguments: [
                targetDSN,
                "-c", "\\copy \(id.qualifiedName) FROM STDIN WITH (FORMAT csv, HEADER)"
            ],
            environment: targetEnv,
            input: exportResult.stdout
        )

        guard importResult.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql COPY import \(id)",
                exitCode: importResult.exitCode,
                stderr: importResult.stderr
            )
        }

        logger.info("Data copied for \(id)")
    }

    private func copyViaPgDump(
        sourceDSN: String,
        targetDSN: String,
        id: ObjectIdentifier,
        sourceEnv: [String: String]
    ) async throws {
        guard let pgDumpPath = shell.which("pg_dump") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump",
                exitCode: -1,
                stderr: "pg_dump not found in PATH"
            )
        }

        guard let pgRestorePath = shell.which("pg_restore") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_restore",
                exitCode: -1,
                stderr: "pg_restore not found in PATH"
            )
        }

        // Export with pg_dump
        let dumpResult = try await shell.run(
            command: pgDumpPath,
            arguments: ["--format=custom", "--data-only", "--table=\(id.qualifiedName)", sourceDSN],
            environment: sourceEnv
        )

        guard dumpResult.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump \(id)",
                exitCode: dumpResult.exitCode,
                stderr: dumpResult.stderr
            )
        }

        // Restore with pg_restore
        let restoreResult = try await shell.run(
            command: pgRestorePath,
            arguments: ["--no-owner", "--data-only", "--dbname=\(targetDSN)"],
            environment: sourceEnv,
            input: dumpResult.stdout
        )

        guard restoreResult.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_restore \(id)",
                exitCode: restoreResult.exitCode,
                stderr: restoreResult.stderr
            )
        }

        logger.info("Data copied via pg_dump for \(id)")
    }

    private func generateDropSQL(for id: ObjectIdentifier) -> String {
        let keyword: String
        switch id.type {
        case .table: keyword = "TABLE"
        case .view: keyword = "VIEW"
        case .materializedView: keyword = "MATERIALIZED VIEW"
        case .sequence: keyword = "SEQUENCE"
        case .function:
            let sig = id.signature ?? "()"
            return "DROP FUNCTION IF EXISTS \(id.qualifiedName)\(sig) CASCADE;"
        case .procedure:
            let sig = id.signature ?? "()"
            return "DROP PROCEDURE IF EXISTS \(id.qualifiedName)\(sig) CASCADE;"
        case .enum, .compositeType: keyword = "TYPE"
        case .schema: keyword = "SCHEMA"
        case .extension: keyword = "EXTENSION"
        case .foreignTable: keyword = "FOREIGN TABLE"
        case .role:
            return "DROP ROLE IF EXISTS \(quoteIdent(id.name));"
        case .aggregate:
            let sig = id.signature ?? "()"
            return "DROP AGGREGATE IF EXISTS \(id.qualifiedName)\(sig) CASCADE;"
        case .operator: keyword = "OPERATOR"
        case .foreignDataWrapper: keyword = "FOREIGN DATA WRAPPER"
        }
        return "DROP \(keyword) IF EXISTS \(id.qualifiedName) CASCADE;"
    }

    private func stepDescription(_ step: CloneStep) -> String {
        switch step {
        case .dropObject(let id): "Drop \(id.type.displayName) \(id)"
        case .createObject(_, let id): "Create \(id.type.displayName) \(id)"
        case .alterObject(_, let id): "Alter \(id.type.displayName) \(id)"
        case .copyData(let id, let method, _, _, _): "Copy data for \(id) via \(method.rawValue)"
        case .grantPermissions(_, let id): "Grant permissions on \(id)"
        case .refreshMaterializedView(let id): "Refresh materialized view \(id)"
        case .enableRLS(_, let id): "Enable RLS on \(id)"
        case .attachPartition(_, let id): "Attach partition \(id)"
        }
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}
