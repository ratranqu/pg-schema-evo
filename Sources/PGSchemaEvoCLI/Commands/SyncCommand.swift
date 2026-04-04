import ArgumentParser
import PGSchemaEvoCore
import Logging

struct SyncCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "sync",
        abstract: "Incrementally sync schema changes from source to target",
        discussion: """
            Compares schemas between source and target databases and applies only
            the necessary changes (CREATE, ALTER, DROP) to bring the target in sync
            with the source. Unlike 'clone', this does not re-create objects that
            already match.

            Examples:
              pg-schema-evo sync --source-dsn postgresql://... --target-dsn postgresql://... \\
                --object table:public.users --object table:public.orders

              pg-schema-evo sync --source-dsn postgresql://... --target-dsn postgresql://... \\
                --type table --schema public --sync-all

              pg-schema-evo sync --source-dsn postgresql://... --target-dsn postgresql://... \\
                --type table --type view --sync-all --drop-extra
            """
    )

    @OptionGroup var source: SourceConnectionOptions
    @OptionGroup var target: TargetConnectionOptions
    @OptionGroup var conflict: ConflictOptions

    @Option(name: .long, help: "Object to sync (format: type:schema.name, repeatable)")
    var object: [String] = []

    @Option(name: .long, help: "Filter by object type when using --sync-all (repeatable)")
    var type: [String] = []

    @Option(name: .long, help: "Filter by schema when using --sync-all")
    var schema: String?

    @Flag(name: .long, help: "Sync all objects matching type/schema filters")
    var syncAll: Bool = false

    @Flag(name: .long, help: "Output dry-run script instead of executing")
    var dryRun: Bool = false

    @Flag(name: .long, help: "Drop objects on target that don't exist on source")
    var dropExtra: Bool = false

    @Flag(name: .long, help: "Drop objects before creating (for objects only in source)")
    var dropExisting: Bool = false

    @Flag(name: .long, help: "Allow dropping columns, constraints, indexes, triggers, and policies extra in target")
    var allowDropColumns: Bool = false

    @Flag(name: .long, help: "Skip interactive confirmation prompt")
    var force: Bool = false

    @Flag(name: .long, help: "Skip pre-flight validation checks")
    var skipPreflight: Bool = false

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

        // Build object specs
        var specs: [ObjectSpec] = []

        if syncAll {
            // When --sync-all, use type/schema filters to discover objects.
            // We still need at least one type filter or object spec.
            if type.isEmpty && object.isEmpty {
                throw ValidationError("Provide --type filters or --object specs with --sync-all")
            }

            // Parse type filters into ObjectSpecs with dummy IDs (used as filters)
            for typeStr in type {
                guard let t = ObjectType(rawValue: typeStr) else {
                    let valid = ObjectType.allCases.map(\.rawValue).joined(separator: ", ")
                    throw ValidationError("Unknown type '\(typeStr)'. Valid: \(valid)")
                }
                // Create a placeholder spec to indicate the type filter
                let id = ObjectIdentifier(
                    type: t,
                    schema: schema ?? (t.isSchemaScoped ? "public" : nil),
                    name: "*"
                )
                specs.append(ObjectSpec(id: id))
            }
        }

        // Parse explicit object specs
        for objStr in object {
            let id = try parseObjectSpecifier(objStr)
            specs.append(ObjectSpec(id: id))
        }

        guard !specs.isEmpty else {
            throw ValidationError("Provide at least one --object or use --sync-all with --type")
        }

        let conflictStrategy = try conflict.resolvedStrategy()

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: specs,
            dryRun: dryRun,
            dropExtra: dropExtra,
            dropIfExists: dropExisting,
            allowDropColumns: allowDropColumns,
            force: force,
            skipPreflight: skipPreflight,
            syncAll: syncAll,
            retries: retries,
            conflictStrategy: conflictStrategy,
            autoAcceptNonDestructive: conflict.yes,
            conflictFilePath: conflict.conflictFile,
            resolveFromPath: conflict.resolveFrom,
            conflictResolutionExplicit: conflict.isExplicit
        )

        let orchestrator = SyncOrchestrator(logger: logger)
        let output = try await orchestrator.execute(job: job)

        if !output.isEmpty {
            print(output)
        }
    }
}
