import PostgresNIO
import Logging

/// PostgresNIO-based implementation of schema introspection.
public final class PGCatalogIntrospector: SchemaIntrospector, @unchecked Sendable {
    private let connection: PostgresConnection
    private let logger: Logger

    public init(connection: PostgresConnection, logger: Logger) {
        self.connection = connection
        self.logger = logger
    }

    public func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Table requires a schema: \(id)")
        }

        logger.debug("Introspecting table \(id)")

        let columns = try await queryColumns(schema: schema, name: id.name)
        if columns.isEmpty {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        let constraints = try await queryConstraints(schema: schema, name: id.name)
        let indexes = try await queryIndexes(schema: schema, name: id.name)
        let triggers = try await queryTriggers(schema: schema, name: id.name)

        return TableMetadata(
            id: id,
            columns: columns,
            constraints: constraints,
            indexes: indexes,
            triggers: triggers
        )
    }

    public func relationSize(_ id: ObjectIdentifier) async throws -> Int? {
        guard let schema = id.schema else { return nil }

        let query: PostgresQuery = """
            SELECT pg_catalog.pg_total_relation_size(c.oid)::bigint AS size_bytes
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema) AND c.relname = \(id.name)
            """

        let rows = try await connection.query(query, logger: logger)
        for try await row in rows {
            let sizeBytes = try row.decode(Int.self)
            return sizeBytes
        }
        return nil
    }

    public func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        var results: [ObjectIdentifier] = []

        let targetTypes = types ?? [.table]

        for type in targetTypes {
            switch type {
            case .table:
                let query: PostgresQuery
                if let schema {
                    query = """
                        SELECT n.nspname, c.relname
                        FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relkind = 'r'
                          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                          AND n.nspname = \(schema)
                        ORDER BY n.nspname, c.relname
                        """
                } else {
                    query = """
                        SELECT n.nspname, c.relname
                        FROM pg_catalog.pg_class c
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relkind = 'r'
                          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                        ORDER BY n.nspname, c.relname
                        """
                }

                let rows = try await connection.query(query, logger: logger)
                for try await row in rows {
                    let (schemaName, tableName) = try row.decode((String, String).self)
                    results.append(ObjectIdentifier(type: .table, schema: schemaName, name: tableName))
                }
            default:
                logger.warning("Listing not yet implemented for type: \(type.displayName)")
            }
        }

        return results
    }

    public func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] {
        guard let schema = id.schema else { return [] }

        let query: PostgresQuery = """
            SELECT grantee, privilege_type, is_grantable::boolean
            FROM information_schema.role_table_grants
            WHERE table_schema = \(schema)
              AND table_name = \(id.name)
              AND grantor != grantee
            ORDER BY grantee, privilege_type
            """

        let rows = try await connection.query(query, logger: logger)
        var grants: [PermissionGrant] = []
        for try await row in rows {
            let (grantee, privilege, isGrantable) = try row.decode((String, String, Bool).self)
            grants.append(PermissionGrant(
                grantee: grantee,
                privilege: privilege,
                isGrantable: isGrantable
            ))
        }
        return grants
    }

    // MARK: - Private query helpers

    private func queryColumns(schema: String, name: String) async throws -> [ColumnInfo] {
        let query: PostgresQuery = """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                NOT a.attnotnull AS is_nullable,
                pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS column_default,
                a.attnum::integer AS ordinal_position,
                CASE WHEN a.attidentity != '' THEN true ELSE false END AS is_identity,
                CASE WHEN a.attidentity = 'a' THEN 'ALWAYS'
                     WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
                     ELSE NULL END AS identity_generation
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = \(schema)
              AND c.relname = \(name)
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """

        let rows = try await connection.query(query, logger: logger)
        var columns: [ColumnInfo] = []
        for try await row in rows {
            let randomAccess = row.makeRandomAccess()
            let columnName = try randomAccess[0].decode(String.self)
            let dataType = try randomAccess[1].decode(String.self)
            let isNullable = try randomAccess[2].decode(Bool.self)
            let columnDefault = try randomAccess[3].decode(String?.self)
            let ordinalPosition = try randomAccess[4].decode(Int.self)
            let isIdentity = try randomAccess[5].decode(Bool.self)
            let identityGeneration = try randomAccess[6].decode(String?.self)
            columns.append(ColumnInfo(
                name: columnName,
                dataType: dataType,
                isNullable: isNullable,
                columnDefault: columnDefault,
                ordinalPosition: ordinalPosition,
                isIdentity: isIdentity,
                identityGeneration: identityGeneration
            ))
        }
        return columns
    }

    private func queryConstraints(schema: String, name: String) async throws -> [ConstraintInfo] {
        let query: PostgresQuery = """
            SELECT
                con.conname AS constraint_name,
                con.contype::text AS constraint_type,
                pg_catalog.pg_get_constraintdef(con.oid, true) AS definition,
                CASE WHEN con.contype = 'f' THEN
                    (SELECT nsp.nspname || '.' || rel.relname
                     FROM pg_catalog.pg_class rel
                     JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
                     WHERE rel.oid = con.confrelid)
                ELSE NULL END AS referenced_table
            FROM pg_catalog.pg_constraint con
            JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(name)
            ORDER BY
                CASE con.contype
                    WHEN 'p' THEN 1
                    WHEN 'u' THEN 2
                    WHEN 'f' THEN 3
                    WHEN 'c' THEN 4
                    WHEN 'x' THEN 5
                END,
                con.conname
            """

        let rows = try await connection.query(query, logger: logger)
        var constraints: [ConstraintInfo] = []
        for try await row in rows {
            let (constraintName, constraintType, definition, referencedTable) =
                try row.decode((String, String, String, String?).self)
            if let type = ConstraintInfo.ConstraintType(rawValue: constraintType) {
                constraints.append(ConstraintInfo(
                    name: constraintName,
                    type: type,
                    definition: definition,
                    referencedTable: referencedTable
                ))
            }
        }
        return constraints
    }

    private func queryIndexes(schema: String, name: String) async throws -> [IndexInfo] {
        let query: PostgresQuery = """
            SELECT
                i.relname AS index_name,
                pg_catalog.pg_get_indexdef(i.oid) AS definition,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = \(schema)
              AND t.relname = \(name)
            ORDER BY i.relname
            """

        let rows = try await connection.query(query, logger: logger)
        var indexes: [IndexInfo] = []
        for try await row in rows {
            let (indexName, definition, isUnique, isPrimary) =
                try row.decode((String, String, Bool, Bool).self)
            indexes.append(IndexInfo(
                name: indexName,
                definition: definition,
                isUnique: isUnique,
                isPrimary: isPrimary
            ))
        }
        return indexes
    }

    private func queryTriggers(schema: String, name: String) async throws -> [TriggerInfo] {
        let query: PostgresQuery = """
            SELECT
                t.tgname AS trigger_name,
                pg_catalog.pg_get_triggerdef(t.oid, true) AS definition
            FROM pg_catalog.pg_trigger t
            JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = \(schema)
              AND c.relname = \(name)
              AND NOT t.tgisinternal
            ORDER BY t.tgname
            """

        let rows = try await connection.query(query, logger: logger)
        var triggers: [TriggerInfo] = []
        for try await row in rows {
            let (triggerName, definition) = try row.decode((String, String).self)
            triggers.append(TriggerInfo(name: triggerName, definition: definition))
        }
        return triggers
    }
}
