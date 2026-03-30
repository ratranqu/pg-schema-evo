/// Protocol for querying PostgreSQL catalog metadata.
public protocol SchemaIntrospector: Sendable {
    /// Retrieve full metadata for a table (columns, constraints, indexes, triggers).
    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata

    /// Approximate size in bytes (pg_total_relation_size). Nil for non-relation objects.
    func relationSize(_ id: ObjectIdentifier) async throws -> Int?

    /// List all objects matching optional filters.
    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier]

    /// Permissions (role, privilege pairs) for an object.
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant]
}
