import ArgumentParser
import PGSchemaEvoCore
import Logging

struct MigrateCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "migrate",
        abstract: "Generate, apply, and track schema migrations",
        discussion: """
            Manage database schema migrations with full version tracking.
            Migrations are stored as paired files: a YAML metadata file and
            a plain SQL file with UP/DOWN sections.

            Examples:
              pg-schema-evo migrate generate --source-dsn ... --target-dsn ... -m "add users index"
              pg-schema-evo migrate apply --target-dsn ...
              pg-schema-evo migrate rollback --target-dsn ...
              pg-schema-evo migrate status --target-dsn ...
            """,
        subcommands: [
            MigrateGenerateCommand.self,
            MigrateApplyCommand.self,
            MigrateRollbackCommand.self,
            MigrateStatusCommand.self,
        ]
    )
}

// MARK: - Generate

struct MigrateGenerateCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "generate",
        abstract: "Generate a migration from diff between source and target"
    )

    @OptionGroup var source: SourceConnectionOptions
    @OptionGroup var target: TargetConnectionOptions

    @Option(name: [.short, .long], help: "Migration description")
    var message: String

    @Option(name: .long, help: "Migration output directory (default: ./migrations)")
    var dir: String = MigrationConfig.defaultDirectory

    @Option(name: .long, help: "Filter by schema name")
    var schema: String?

    @Option(name: .long, help: "Filter by object type (repeatable)")
    var type: [String] = []

    @Flag(name: .long, help: "Include destructive DROP statements")
    var includeDestructive: Bool = false

    @Flag(name: [.short, .long], help: "Enable verbose logging output")
    var verbose: Bool = false

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = verbose ? .debug : .warning

        guard !source.sourceDsn.isEmpty else {
            throw ValidationError("--source-dsn is required")
        }
        guard !target.targetDsn.isEmpty else {
            throw ValidationError("--target-dsn is required")
        }

        let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)
        let targetConfig = try ConnectionConfig.fromDSN(target.targetDsn)

        let sourceConn = try await PostgresConnectionHelper.connect(config: sourceConfig, logger: logger)
        let targetConn = try await PostgresConnectionHelper.connect(config: targetConfig, logger: logger)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: logger)

        var objectTypes: [ObjectType]?
        if !type.isEmpty {
            objectTypes = try type.map { typeStr in
                guard let t = ObjectType(rawValue: typeStr) else {
                    let valid = ObjectType.allCases.map(\.rawValue).joined(separator: ", ")
                    throw ValidationError("Unknown type '\(typeStr)'. Valid: \(valid)")
                }
                return t
            }
        }

        let differ = SchemaDiffer(logger: logger)
        let diff = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: schema,
            types: objectTypes
        )

        try? await sourceConn.close()
        try? await targetConn.close()

        guard !diff.isEmpty else {
            print("Schemas are identical. No migration needed.")
            return
        }

        let generator = MigrationGenerator(logger: logger)
        let (migration, sql) = generator.generate(
            from: diff,
            description: message,
            includeDestructive: includeDestructive
        )

        let fileManager = MigrationFileManager(directory: dir)
        try fileManager.write(migration: migration, sql: sql)

        print("Generated migration: \(migration.id)")
        print("  SQL:  \(fileManager.sqlPath(for: migration.id))")
        print("  Meta: \(fileManager.yamlPath(for: migration.id))")
        if !migration.irreversibleChanges.isEmpty {
            print("  Warning: Contains irreversible changes:")
            for change in migration.irreversibleChanges {
                print("    - \(change)")
            }
        }
    }
}

// MARK: - Apply

struct MigrateApplyCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "apply",
        abstract: "Apply pending migrations to the target database"
    )

    @OptionGroup var target: TargetConnectionOptions

    @Option(name: .long, help: "Migration directory (default: ./migrations)")
    var dir: String = MigrationConfig.defaultDirectory

    @Option(name: .long, help: "Tracking table name")
    var trackingTable: String = MigrationConfig.defaultTrackingTable

    @Option(name: .long, help: "Tracking table schema")
    var trackingSchema: String = MigrationConfig.defaultTrackingSchema

    @Option(name: [.short, .long], help: "Max number of migrations to apply")
    var count: Int?

    @Flag(name: .long, help: "Apply even if checksum mismatch detected")
    var force: Bool = false

    @Flag(name: .long, help: "Show SQL without executing")
    var dryRun: Bool = false

    @Flag(name: [.short, .long], help: "Enable verbose logging output")
    var verbose: Bool = false

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = verbose ? .debug : .warning

        guard !target.targetDsn.isEmpty else {
            throw ValidationError("--target-dsn is required")
        }

        let config = MigrationConfig(
            directory: dir,
            trackingTable: trackingTable,
            trackingSchema: trackingSchema
        )

        let applicator = MigrationApplicator(config: config, logger: logger)
        let applied = try await applicator.apply(
            targetDSN: target.targetDsn,
            count: count,
            force: force,
            dryRun: dryRun
        )

        if applied.isEmpty {
            print("No migrations to apply.")
        } else {
            print("Applied \(applied.count) migration(s):")
            for id in applied {
                print("  \(id)")
            }
        }
    }
}

// MARK: - Rollback

struct MigrateRollbackCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "rollback",
        abstract: "Rollback the last N applied migrations"
    )

    @OptionGroup var target: TargetConnectionOptions

    @Option(name: .long, help: "Migration directory (default: ./migrations)")
    var dir: String = MigrationConfig.defaultDirectory

    @Option(name: .long, help: "Tracking table name")
    var trackingTable: String = MigrationConfig.defaultTrackingTable

    @Option(name: .long, help: "Tracking table schema")
    var trackingSchema: String = MigrationConfig.defaultTrackingSchema

    @Option(name: [.short, .long], help: "Number of migrations to rollback (default: 1)")
    var count: Int = 1

    @Flag(name: .long, help: "Rollback even if migration has irreversible changes")
    var force: Bool = false

    @Flag(name: .long, help: "Show SQL without executing")
    var dryRun: Bool = false

    @Flag(name: [.short, .long], help: "Enable verbose logging output")
    var verbose: Bool = false

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = verbose ? .debug : .warning

        guard !target.targetDsn.isEmpty else {
            throw ValidationError("--target-dsn is required")
        }

        let config = MigrationConfig(
            directory: dir,
            trackingTable: trackingTable,
            trackingSchema: trackingSchema
        )

        let applicator = MigrationApplicator(config: config, logger: logger)
        let rolledBack = try await applicator.rollback(
            targetDSN: target.targetDsn,
            count: count,
            force: force,
            dryRun: dryRun
        )

        if rolledBack.isEmpty {
            print("No migrations to rollback.")
        } else {
            print("Rolled back \(rolledBack.count) migration(s):")
            for id in rolledBack {
                print("  \(id)")
            }
        }
    }
}

// MARK: - Status

struct MigrateStatusCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "status",
        abstract: "Show migration status"
    )

    @OptionGroup var target: TargetConnectionOptions

    @Option(name: .long, help: "Migration directory (default: ./migrations)")
    var dir: String = MigrationConfig.defaultDirectory

    @Option(name: .long, help: "Tracking table name")
    var trackingTable: String = MigrationConfig.defaultTrackingTable

    @Option(name: .long, help: "Tracking table schema")
    var trackingSchema: String = MigrationConfig.defaultTrackingSchema

    @Flag(name: [.short, .long], help: "Enable verbose logging output")
    var verbose: Bool = false

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = verbose ? .debug : .warning

        guard !target.targetDsn.isEmpty else {
            throw ValidationError("--target-dsn is required")
        }

        let config = MigrationConfig(
            directory: dir,
            trackingTable: trackingTable,
            trackingSchema: trackingSchema
        )

        let applicator = MigrationApplicator(config: config, logger: logger)
        let status = try await applicator.status(targetDSN: target.targetDsn)
        print(status.render())
    }
}
