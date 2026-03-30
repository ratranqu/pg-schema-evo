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

        let rows = try await connection.query(
            PostgresQuery(
                unsafeSQL: TableQueries.relationSize,
                binds: [schema, id.name]
            ),
            logger: logger
        )

        for try await row in rows {
            let (sizeBytes,) = try row.decode((Int.self), context: .default)
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
                let rows = try await connection.query(
                    PostgresQuery(
                        unsafeSQL: TableQueries.listTables,
                        binds: [schema as String?]
                    ),
                    logger: logger
                )
                for try await row in rows {
                    let (schemaName, tableName) = try row.decode((String, String).self, context: .default)
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

        let rows = try await connection.query(
            PostgresQuery(
                unsafeSQL: TableQueries.permissions,
                binds: [schema, id.name]
            ),
            logger: logger
        )

        var grants: [PermissionGrant] = []
        for try await row in rows {
            let (grantee, privilege, isGrantable) = try row.decode(
                (String, String, Bool).self, context: .default
            )
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
        let rows = try await connection.query(
            PostgresQuery(
                unsafeSQL: TableQueries.columns,
                binds: [schema, name]
            ),
            logger: logger
        )

        var columns: [ColumnInfo] = []
        for try await row in rows {
            let (columnName, dataType, isNullable, columnDefault, ordinalPosition, isIdentity, identityGeneration) =
                try row.decode(
                    (String, String, Bool, String?, Int, Bool, String?).self,
                    context: .default
                )
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
        let rows = try await connection.query(
            PostgresQuery(
                unsafeSQL: TableQueries.constraints,
                binds: [schema, name]
            ),
            logger: logger
        )

        var constraints: [ConstraintInfo] = []
        for try await row in rows {
            let (constraintName, constraintType, definition, referencedTable) =
                try row.decode(
                    (String, String, String, String?).self,
                    context: .default
                )
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
        let rows = try await connection.query(
            PostgresQuery(
                unsafeSQL: TableQueries.indexes,
                binds: [schema, name]
            ),
            logger: logger
        )

        var indexes: [IndexInfo] = []
        for try await row in rows {
            let (indexName, definition, isUnique, isPrimary) =
                try row.decode(
                    (String, String, Bool, Bool).self,
                    context: .default
                )
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
        let rows = try await connection.query(
            PostgresQuery(
                unsafeSQL: TableQueries.triggers,
                binds: [schema, name]
            ),
            logger: logger
        )

        var triggers: [TriggerInfo] = []
        for try await row in rows {
            let (triggerName, definition) =
                try row.decode((String, String).self, context: .default)
            triggers.append(TriggerInfo(name: triggerName, definition: definition))
        }
        return triggers
    }
}
