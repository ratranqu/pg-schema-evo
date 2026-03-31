import ArgumentParser
import PGSchemaEvoCore
import Logging

struct DataSyncCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "data-sync",
        abstract: "Incremental data synchronization between databases",
        discussion: """
            Syncs row-level data changes from source to target using UPSERT.
            Requires a tracking column (timestamp or monotonic ID) per table
            and a primary key for conflict resolution.

            Workflow:
              1. Initialize: data-sync init --source-dsn ... --object table:public.orders --tracking-column orders:updated_at
              2. Run sync:   data-sync run  --source-dsn ... --target-dsn ... --state-file .pg-schema-evo-sync-state.yaml
            """,
        subcommands: [InitSubcommand.self, RunSubcommand.self]
    )

    struct InitSubcommand: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "init",
            abstract: "Initialize sync state by capturing current tracking values"
        )

        @OptionGroup var source: SourceConnectionOptions

        @Option(name: .long, help: "Table to sync (format: type:schema.name, repeatable)")
        var object: [String] = []

        @Option(name: .long, help: "Tracking column per table (format: table_name:column_name, repeatable)")
        var trackingColumn: [String] = []

        @Option(name: .long, help: "Path to sync state file")
        var stateFile: String = ".pg-schema-evo-sync-state.yaml"

        @Flag(name: [.short, .long], help: "Enable verbose logging output")
        var verbose: Bool = false

        func run() async throws {
            var logger = Logger(label: "pg-schema-evo")
            logger.logLevel = verbose ? .debug : .info

            guard !source.sourceDsn.isEmpty else {
                throw ValidationError("--source-dsn is required")
            }

            let tables = try parseTableConfigs(objects: object, trackingColumns: trackingColumn)
            guard !tables.isEmpty else {
                throw ValidationError("Provide at least one --object with a corresponding --tracking-column")
            }

            let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)

            let job = DataSyncJob(
                source: sourceConfig,
                target: sourceConfig, // target not needed for init
                tables: tables,
                stateFilePath: stateFile
            )

            let orchestrator = DataSyncOrchestrator(logger: logger)
            let output = try await orchestrator.initialize(job: job)
            print(output)
        }
    }

    struct RunSubcommand: AsyncParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "run",
            abstract: "Run incremental data sync using the state file"
        )

        @OptionGroup var source: SourceConnectionOptions
        @OptionGroup var target: TargetConnectionOptions

        @Option(name: .long, help: "Path to sync state file")
        var stateFile: String = ".pg-schema-evo-sync-state.yaml"

        @Flag(name: .long, help: "Detect and delete rows on target that no longer exist on source")
        var detectDeletes: Bool = false

        @Flag(name: .long, help: "Output dry-run script instead of executing")
        var dryRun: Bool = false

        @Flag(name: .long, help: "Skip interactive confirmation prompt")
        var force: Bool = false

        @Option(name: .long, help: "Maximum retry attempts (default: 3)")
        var retries: Int = 3

        @Flag(name: [.short, .long], help: "Enable verbose logging output")
        var verbose: Bool = false

        func run() async throws {
            var logger = Logger(label: "pg-schema-evo")
            logger.logLevel = verbose ? .debug : .info

            guard !source.sourceDsn.isEmpty else {
                throw ValidationError("--source-dsn is required")
            }
            guard !target.targetDsn.isEmpty else {
                throw ValidationError("--target-dsn is required")
            }

            let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)
            let targetConfig = try ConnectionConfig.fromDSN(target.targetDsn)

            let job = DataSyncJob(
                source: sourceConfig,
                target: targetConfig,
                tables: [], // tables loaded from state file
                stateFilePath: stateFile,
                dryRun: dryRun,
                detectDeletes: detectDeletes,
                force: force,
                retries: retries
            )

            let orchestrator = DataSyncOrchestrator(logger: logger)

            // Confirmation prompt unless --force or --dry-run
            if !dryRun && !force {
                let progress = ProgressReporter(totalSteps: 0)
                let response = progress.reportConfirmation(
                    targetDSN: targetConfig.toDSN(maskPassword: true)
                )
                guard response.lowercased() == "yes" else {
                    print("Aborted by user.")
                    return
                }
            }

            let output = try await orchestrator.run(job: job)
            if !output.isEmpty {
                print(output)
            }
        }
    }
}

/// Parse --object and --tracking-column flags into DataSyncTableConfig array.
///
/// --tracking-column format: "table_name:column_name" (e.g. "orders:updated_at")
private func parseTableConfigs(
    objects: [String],
    trackingColumns: [String]
) throws -> [DataSyncTableConfig] {
    // Build a map of table name -> tracking column
    var trackingMap: [String: String] = [:]
    for tc in trackingColumns {
        let parts = tc.split(separator: ":", maxSplits: 1).map(String.init)
        guard parts.count == 2 else {
            throw ValidationError(
                "Invalid --tracking-column format '\(tc)'. Expected 'table_name:column_name'."
            )
        }
        trackingMap[parts[0]] = parts[1]
    }

    var configs: [DataSyncTableConfig] = []
    for objStr in objects {
        let id = try parseObjectSpecifier(objStr)
        guard id.type == .table else {
            throw ValidationError("data-sync only supports tables, got '\(id.type.displayName)' for \(id)")
        }

        guard let column = trackingMap[id.name] else {
            throw ValidationError(
                "Missing --tracking-column for table '\(id.name)'. " +
                "Provide --tracking-column \(id.name):column_name."
            )
        }

        configs.append(DataSyncTableConfig(id: id, trackingColumn: column))
    }

    return configs
}
