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

    /// Execute all clone steps wrapped in a single transaction.
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

        // Begin transaction
        try await executePsql(
            psqlPath: psqlPath, dsn: targetDSN, sql: "BEGIN;",
            env: env, description: "BEGIN transaction"
        )

        do {
            for (index, step) in steps.enumerated() {
                let stepNum = index + 1
                let desc = stepDescription(step)
                progress.reportStep(stepNum, description: desc)

                try await executeStep(
                    step: step,
                    psqlPath: psqlPath,
                    targetDSN: targetDSN,
                    sourceDSN: sourceDSN,
                    env: env,
                    sourceEnv: sourceEnv
                )

                progress.reportStepComplete(stepNum, description: desc)
            }

            // Commit
            try await executePsql(
                psqlPath: psqlPath, dsn: targetDSN, sql: "COMMIT;",
                env: env, description: "COMMIT transaction"
            )
        } catch {
            // Rollback on failure
            logger.warning("Rolling back transaction due to error...")
            try? await executePsql(
                psqlPath: psqlPath, dsn: targetDSN, sql: "ROLLBACK;",
                env: env, description: "ROLLBACK transaction"
            )
            throw error
        }

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

    // MARK: - Private

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
