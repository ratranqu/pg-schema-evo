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

        let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)

        let pgConfig = PostgresConnection.Configuration(
            host: sourceConfig.host,
            port: sourceConfig.port,
            username: sourceConfig.username,
            password: sourceConfig.password,
            database: sourceConfig.database,
            tls: .disable
        )

        let connection = try await PostgresConnection.connect(
            configuration: pgConfig,
            id: 1,
            logger: logger
        )

        defer {
            Task { try? await connection.close() }
        }

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
    }
}
