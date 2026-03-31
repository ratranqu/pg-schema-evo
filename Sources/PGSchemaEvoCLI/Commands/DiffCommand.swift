import ArgumentParser
import PGSchemaEvoCore
import Logging

struct DiffCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "diff",
        abstract: "Compare schemas between source and target databases",
        discussion: """
            Shows differences between source and target databases.
            Use --sql to output a migration script instead of a text diff.

            Examples:
              pg-schema-evo diff --source-dsn postgresql://... --target-dsn postgresql://...
              pg-schema-evo diff --source-dsn postgresql://... --target-dsn postgresql://... --sql
              pg-schema-evo diff --source-dsn postgresql://... --target-dsn postgresql://... --schema public --type table
            """
    )

    @OptionGroup var source: SourceConnectionOptions
    @OptionGroup var target: TargetConnectionOptions

    @Option(name: .long, help: "Filter by schema name")
    var schema: String?

    @Option(name: .long, help: "Filter by object type (repeatable)")
    var type: [String] = []

    @Flag(name: .long, help: "Output SQL migration script instead of text diff")
    var sql: Bool = false

    @Flag(name: .long, help: "Include destructive DROP statements in SQL output (columns, constraints, indexes, triggers, policies, objects)")
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
        let result = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: schema,
            types: objectTypes
        )

        if sql {
            print(result.renderMigrationSQL(includeDestructive: includeDestructive))
        } else {
            print(result.renderText())
        }

        try? await sourceConn.close()
        try? await targetConn.close()
    }
}
