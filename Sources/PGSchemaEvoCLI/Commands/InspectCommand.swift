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

        guard !source.sourceDsn.isEmpty else {
            throw ValidationError("--source-dsn is required")
        }
        let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)
        let objectId = try parseObjectSpecifier(object)

        let connection = try await PostgresConnectionHelper.connect(
            config: sourceConfig,
            logger: logger
        )

        let introspector = PGCatalogIntrospector(connection: connection, logger: logger)

        switch objectId.type {
        case .table:
            let metadata = try await introspector.describeTable(objectId)
            printTableMetadata(metadata)
            if let size = try await introspector.relationSize(objectId) {
                print("Size: \(formatBytes(size))")
            }

        case .view:
            let metadata = try await introspector.describeView(objectId)
            print("View: \(metadata.id.qualifiedName)")
            print("\nDefinition:")
            print("  \(metadata.definition)")
            if !metadata.columns.isEmpty {
                print("\nColumns:")
                for col in metadata.columns {
                    print("  \(col.name) \(col.dataType)")
                }
            }

        case .materializedView:
            let metadata = try await introspector.describeMaterializedView(objectId)
            print("Materialized View: \(metadata.id.qualifiedName)")
            print("\nDefinition:")
            print("  \(metadata.definition)")
            if !metadata.indexes.isEmpty {
                print("\nIndexes:")
                for idx in metadata.indexes {
                    print("  \(idx.name)")
                }
            }
            if let size = try await introspector.relationSize(objectId) {
                print("Size: \(formatBytes(size))")
            }

        case .sequence:
            let metadata = try await introspector.describeSequence(objectId)
            print("Sequence: \(metadata.id.qualifiedName)")
            print("  Type: \(metadata.dataType)")
            print("  Start: \(metadata.startValue)  Increment: \(metadata.increment)")
            print("  Min: \(metadata.minValue)  Max: \(metadata.maxValue)")
            print("  Cache: \(metadata.cacheSize)  Cycle: \(metadata.isCycled)")
            if let owned = metadata.ownedByColumn {
                print("  Owned by: \(owned)")
            }

        case .enum:
            let metadata = try await introspector.describeEnum(objectId)
            print("Enum: \(metadata.id.qualifiedName)")
            print("  Labels: \(metadata.labels.joined(separator: ", "))")

        case .function, .procedure:
            let metadata = try await introspector.describeFunction(objectId)
            print("\(objectId.type.displayName.capitalized): \(metadata.id.qualifiedName)")
            if let ret = metadata.returnType {
                print("  Returns: \(ret)")
            }
            print("  Language: \(metadata.language)")
            print("  Volatility: \(metadata.volatility)")
            print("  Strict: \(metadata.isStrict)")
            print("  Security definer: \(metadata.isSecurityDefiner)")

        case .schema:
            let metadata = try await introspector.describeSchema(objectId)
            print("Schema: \(metadata.id.name)")
            print("  Owner: \(metadata.owner)")

        case .role:
            let metadata = try await introspector.describeRole(objectId)
            print("Role: \(metadata.id.name)")
            print("  Login: \(metadata.canLogin)  Superuser: \(metadata.isSuperuser)")
            print("  CreateDB: \(metadata.canCreateDB)  CreateRole: \(metadata.canCreateRole)")
            if !metadata.memberOf.isEmpty {
                print("  Member of: \(metadata.memberOf.joined(separator: ", "))")
            }

        case .compositeType:
            let metadata = try await introspector.describeCompositeType(objectId)
            print("Composite Type: \(metadata.id.qualifiedName)")
            print("\nAttributes:")
            for attr in metadata.attributes {
                print("  \(attr.name) \(attr.dataType)")
            }

        case .extension:
            let metadata = try await introspector.describeExtension(objectId)
            print("Extension: \(metadata.id.name)")
            print("  Version: \(metadata.version)")
            if let schema = metadata.installedSchema {
                print("  Schema: \(schema)")
            }

        default:
            print("Inspection not yet supported for type: \(objectId.type.displayName)")
        }

        // Show permissions for schema-scoped objects
        if objectId.type.isSchemaScoped {
            let grants = try await introspector.permissions(for: objectId)
            if !grants.isEmpty {
                print("\nPermissions:")
                for grant in grants {
                    var line = "  \(grant.grantee): \(grant.privilege)"
                    if grant.isGrantable { line += " (WITH GRANT OPTION)" }
                    print(line)
                }
            }
        }

        try? await connection.close()
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
