import PostgresNIO
import Logging

/// Coordinates incremental data sync: detects changed rows via a tracking column,
/// fetches deltas from the source, and applies them to the target via UPSERT.
public struct DataSyncOrchestrator: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    // MARK: - Init (seed the state file)

    /// Initialize a sync state file by querying MAX(tracking_column) per table.
    public func initialize(job: DataSyncJob) async throws -> String {
        logger.info("Initializing data sync state for \(job.tables.count) table(s)")

        let sourceConn = try await PostgresConnectionHelper.connect(
            config: job.source,
            logger: logger
        )
        defer { Task { try? await sourceConn.close() } }

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: logger)
        let shell = ShellRunner()
        let sqlGen = UpsertSQLGenerator()

        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql", exitCode: -1,
                stderr: "psql not found in PATH. Install postgresql-client."
            )
        }

        var state = DataSyncState()

        do {
            for tableConfig in job.tables {
                let id = tableConfig.id
                let trackingColumn = tableConfig.trackingColumn

                // Validate table exists and has the tracking column
                let metadata = try await introspector.describeTable(id)
                guard metadata.columns.contains(where: { $0.name == trackingColumn }) else {
                    throw PGSchemaEvoError.trackingColumnNotFound(table: id, column: trackingColumn)
                }

                // Validate table has a primary key
                let pkColumns = try await introspector.primaryKeyColumns(for: id)
                guard !pkColumns.isEmpty else {
                    throw PGSchemaEvoError.noPrimaryKey(id)
                }

                // Query MAX(tracking_column)
                let query = sqlGen.generateMaxTrackingQuery(table: id, trackingColumn: trackingColumn)
                let result = try await shell.run(
                    command: psqlPath,
                    arguments: [job.source.toDSN(), "-t", "-A", "-c", query],
                    environment: job.source.environment()
                )

                guard result.succeeded else {
                    throw PGSchemaEvoError.shellCommandFailed(
                        command: "psql (MAX query)",
                        exitCode: result.exitCode,
                        stderr: result.stderr
                    )
                }

                let maxValue = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)

                let tableKey = "\(id.schema ?? "public").\(id.name)"
                state.tables[tableKey] = DataSyncTableState(
                    column: trackingColumn,
                    lastValue: maxValue
                )

                logger.info("Table \(tableKey): tracking '\(trackingColumn)', initial value = \(maxValue)")
            }
        } catch {
            throw error
        }

        // Save state file
        let stateStore = DataSyncStateStore()
        try stateStore.save(state: state, path: job.stateFilePath)

        var output = "Sync state initialized at '\(job.stateFilePath)':\n"
        for (key, tableState) in state.tables.sorted(by: { $0.key < $1.key }) {
            output += "  \(key): \(tableState.column) = \(tableState.lastValue)\n"
        }
        return output
    }

    // MARK: - Run (incremental sync)

    /// Execute an incremental data sync using the state file.
    public func run(job: DataSyncJob) async throws -> String {
        let stateStore = DataSyncStateStore()
        var state = try stateStore.load(path: job.stateFilePath)

        logger.info("Starting incremental data sync for \(state.tables.count) table(s)")

        let sourceConn = try await PostgresConnectionHelper.connect(
            config: job.source,
            logger: logger
        )

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: logger)
        let shell = ShellRunner()
        let sqlGen = UpsertSQLGenerator()

        guard let psqlPath = shell.which("psql") else {
            try? await sourceConn.close()
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql", exitCode: -1,
                stderr: "psql not found in PATH. Install postgresql-client."
            )
        }

        let sourceDSN = job.source.toDSN()
        let targetDSN = job.target.toDSN()
        let sourceEnv = job.source.environment()
        let targetEnv = job.target.environment()

        var summary: [String] = []
        let progress = ProgressReporter(totalSteps: state.tables.count)

        do {
            for (index, entry) in state.tables.sorted(by: { $0.key < $1.key }).enumerated() {
                let tableKey = entry.key
                let tableState = entry.value
                let stepNum = index + 1

                progress.reportStep(stepNum, description: "Syncing \(tableKey)")

                // Parse table key back to ObjectIdentifier
                let parts = tableKey.split(separator: ".", maxSplits: 1).map(String.init)
                let id: ObjectIdentifier
                if parts.count == 2 {
                    id = ObjectIdentifier(type: .table, schema: parts[0], name: parts[1])
                } else {
                    id = ObjectIdentifier(type: .table, schema: "public", name: tableKey)
                }

                // Introspect metadata
                let metadata = try await introspector.describeTable(id)
                let pkColumns = try await introspector.primaryKeyColumns(for: id)
                guard !pkColumns.isEmpty else {
                    throw PGSchemaEvoError.noPrimaryKey(id)
                }

                // Fetch incremental data from source
                let copyCommand = sqlGen.generateIncrementalCopyCommand(
                    table: id,
                    trackingColumn: tableState.column,
                    lastValue: tableState.lastValue
                )

                let exportResult = try await shell.run(
                    command: psqlPath,
                    arguments: [sourceDSN, "-c", copyCommand],
                    environment: sourceEnv
                )

                guard exportResult.succeeded else {
                    throw PGSchemaEvoError.shellCommandFailed(
                        command: "psql (incremental export \(id))",
                        exitCode: exportResult.exitCode,
                        stderr: exportResult.stderr
                    )
                }

                let csvData = exportResult.stdout

                // Check if there are any rows (header-only means no changes)
                let lineCount = csvData.split(separator: "\n", omittingEmptySubsequences: true).count
                if lineCount <= 1 {
                    summary.append("\(tableKey): no changes")
                    progress.reportStepComplete(stepNum, description: "\(tableKey): no changes")
                    continue
                }

                let rowCount = max(lineCount - 1, 0)  // subtract header, guard against negative

                // Fetch all source PKs for delete detection if requested
                var deletePKData: String?
                if job.detectDeletes {
                    let pkExportCommand = sqlGen.generatePKExportCommand(table: id, pkColumns: pkColumns)
                    let pkResult = try await shell.run(
                        command: psqlPath,
                        arguments: [sourceDSN, "-c", pkExportCommand],
                        environment: sourceEnv
                    )
                    guard pkResult.succeeded else {
                        throw PGSchemaEvoError.shellCommandFailed(
                            command: "psql (PK export \(id))",
                            exitCode: pkResult.exitCode,
                            stderr: pkResult.stderr
                        )
                    }
                    deletePKData = pkResult.stdout
                }

                // Build the transaction script for this table
                let script = sqlGen.buildTableSyncScript(
                    table: id,
                    columns: metadata.columns,
                    pkColumns: pkColumns,
                    csvData: csvData,
                    detectDeletes: job.detectDeletes,
                    deletePKData: deletePKData
                )

                if job.dryRun {
                    summary.append("\(tableKey): \(rowCount) row(s) to sync")
                    print("-- Data sync script for \(tableKey)")
                    print(script)
                } else {
                    // Execute the transaction against the target
                    let execResult = try await shell.run(
                        command: psqlPath,
                        arguments: [targetDSN, "--set", "ON_ERROR_STOP=1", "-X"],
                        environment: targetEnv,
                        input: script
                    )

                    guard execResult.succeeded else {
                        throw PGSchemaEvoError.shellCommandFailed(
                            command: "psql (data-sync \(id))",
                            exitCode: execResult.exitCode,
                            stderr: execResult.stderr
                        )
                    }

                    // Query new MAX(tracking_column) from source
                    let maxQuery = sqlGen.generateMaxTrackingQuery(
                        table: id,
                        trackingColumn: tableState.column
                    )
                    let maxResult = try await shell.run(
                        command: psqlPath,
                        arguments: [sourceDSN, "-t", "-A", "-c", maxQuery],
                        environment: sourceEnv
                    )

                    if maxResult.succeeded {
                        let newMax = maxResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                        if !newMax.isEmpty {
                            state.tables[tableKey] = DataSyncTableState(
                                column: tableState.column,
                                lastValue: newMax
                            )
                        } else {
                            logger.warning("Empty MAX value for \(tableKey), keeping previous state")
                        }
                    } else {
                        logger.warning("Failed to query MAX(\(tableState.column)) for \(tableKey): \(maxResult.stderr). State not updated for this table.")
                    }

                    summary.append("\(tableKey): \(rowCount) row(s) synced")
                }

                progress.reportStepComplete(stepNum, description: "\(tableKey): \(rowCount) row(s)")
            }
        } catch {
            try? await sourceConn.close()
            throw error
        }

        try? await sourceConn.close()

        // Persist state atomically after all tables are synced
        if !job.dryRun {
            try stateStore.save(state: state, path: job.stateFilePath)
        }

        if summary.isEmpty {
            return "No tables to sync."
        }

        let header = job.dryRun ? "Dry-run summary:" : "Data sync complete:"
        return header + "\n" + summary.map { "  \($0)" }.joined(separator: "\n")
    }
}
