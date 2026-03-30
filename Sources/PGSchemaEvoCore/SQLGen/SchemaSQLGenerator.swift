/// Generates DDL for schemas, roles, and extensions.
public struct SchemaSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.schema, .role, .extension] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        if let schema = metadata as? SchemaMetadata {
            return generateCreateSchema(schema)
        } else if let role = metadata as? RoleMetadata {
            return generateCreateRole(role)
        } else if let ext = metadata as? ExtensionMetadata {
            return generateCreateExtension(ext)
        }
        throw PGSchemaEvoError.sqlGenerationFailed(
            metadata.id,
            reason: "Expected SchemaMetadata, RoleMetadata, or ExtensionMetadata"
        )
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        switch id.type {
        case .schema:
            return "DROP SCHEMA IF EXISTS \(quoteIdent(id.name)) CASCADE;"
        case .role:
            return "DROP ROLE IF EXISTS \(quoteIdent(id.name));"
        case .extension:
            return "DROP EXTENSION IF EXISTS \(quoteIdent(id.name)) CASCADE;"
        default:
            return "-- unsupported drop for \(id)"
        }
    }

    private func generateCreateSchema(_ schema: SchemaMetadata) -> String {
        var sql = "CREATE SCHEMA IF NOT EXISTS \(quoteIdent(schema.id.name));\n"
        sql += "ALTER SCHEMA \(quoteIdent(schema.id.name)) OWNER TO \(quoteIdent(schema.owner));"
        return sql
    }

    private func generateCreateRole(_ role: RoleMetadata) -> String {
        var options: [String] = []

        if role.canLogin { options.append("LOGIN") } else { options.append("NOLOGIN") }
        if role.isSuperuser { options.append("SUPERUSER") }
        if role.canCreateDB { options.append("CREATEDB") }
        if role.canCreateRole { options.append("CREATEROLE") }
        if role.connectionLimit >= 0 { options.append("CONNECTION LIMIT \(role.connectionLimit)") }

        var sql = """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '\(escapeSQLString(role.id.name))') THEN
                    CREATE ROLE \(quoteIdent(role.id.name)) \(options.joined(separator: " "));
                END IF;
            END $$;
            """

        for memberOf in role.memberOf {
            sql += "\nGRANT \(quoteIdent(memberOf)) TO \(quoteIdent(role.id.name));"
        }

        return sql
    }

    private func generateCreateExtension(_ ext: ExtensionMetadata) -> String {
        var sql = "CREATE EXTENSION IF NOT EXISTS \(quoteIdent(ext.id.name))"
        if let schema = ext.installedSchema, schema != "public" {
            sql += " SCHEMA \(quoteIdent(schema))"
        }
        sql += " VERSION '\(escapeSQLString(ext.version))';"
        return sql
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    private func escapeSQLString(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}
