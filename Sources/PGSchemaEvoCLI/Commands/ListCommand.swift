import ArgumentParser
import PGSchemaEvoCore
import PostgresNIO
import Logging

struct ListCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "list",
        abstract: "List database objects in a cluster"
    )

    @OptionGroup var source: SourceConnectionOptions

    @Option(name: .long, help: "Filter by schema name")
    var schema: String?

    @Option(name: .long, help: "Filter by object type (repeatable)")
    var type: [String] = []

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = .warning

        guard !source.sourceDsn.isEmpty else {
            throw ValidationError("--source-dsn is required")
        }
        let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)

        let connection = try await PostgresConnectionHelper.connect(
            config: sourceConfig,
            logger: logger
        )

        let introspector = PGCatalogIntrospector(connection: connection, logger: logger)

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

        let objects = try await introspector.listObjects(schema: schema, types: objectTypes)

        for obj in objects {
            print(obj.description)
        }

        if objects.isEmpty {
            print("No objects found.")
        }

        try? await connection.close()
    }
}
