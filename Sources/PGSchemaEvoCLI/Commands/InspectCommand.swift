import ArgumentParser
import PGSchemaEvoCore
import PostgresNIO
import Logging

struct InspectCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "inspect",
        abstract: "Show metadata for a database object"
    )

    @OptionGroup var source: SourceConnectionOptions

    @Option(name: .long, help: "Object to inspect (e.g. table:public.users)")
    var object: String

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = .warning

        let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)
        let objectId = try parseObjectSpecifier(object)

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

        switch objectId.type {
        case .table:
            let metadata = try await introspector.describeTable(objectId)
            printTableMetadata(metadata)

            if let size = try await introspector.relationSize(objectId) {
                print("Size: \(formatBytes(size))")
            }
        default:
            print("Inspection not yet supported for type: \(objectId.type.displayName)")
        }
    }

    private func printTableMetadata(_ table: TableMetadata) {
        print("Table: \(table.id.qualifiedName)")
        print("")
        print("Columns:")
        for col in table.columns.sorted(by: { $0.ordinalPosition < $1.ordinalPosition }) {
            var line = "  \(col.name) \(col.dataType)"
            if !col.isNullable { line += " NOT NULL" }
            if let def = col.columnDefault { line += " DEFAULT \(def)" }
            print(line)
        }

        if !table.constraints.isEmpty {
            print("")
            print("Constraints:")
            for con in table.constraints {
                print("  \(con.name) [\(con.type.rawValue)] \(con.definition)")
            }
        }

        if !table.indexes.isEmpty {
            print("")
            print("Indexes:")
            for idx in table.indexes {
                var flags: [String] = []
                if idx.isPrimary { flags.append("PRIMARY") }
                if idx.isUnique { flags.append("UNIQUE") }
                let flagStr = flags.isEmpty ? "" : " [\(flags.joined(separator: ", "))]"
                print("  \(idx.name)\(flagStr)")
            }
        }

        if !table.triggers.isEmpty {
            print("")
            print("Triggers:")
            for trig in table.triggers {
                print("  \(trig.name)")
            }
        }
    }

    private func formatBytes(_ bytes: Int) -> String {
        if bytes >= 1_073_741_824 {
            return String(format: "%.1f GB", Double(bytes) / 1_073_741_824.0)
        } else if bytes >= 1_048_576 {
            return String(format: "%.1f MB", Double(bytes) / 1_048_576.0)
        } else if bytes >= 1024 {
            return String(format: "%.1f KB", Double(bytes) / 1024.0)
        }
        return "\(bytes) B"
    }
}
