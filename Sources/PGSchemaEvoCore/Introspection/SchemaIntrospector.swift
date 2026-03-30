/// Protocol for querying PostgreSQL catalog metadata.
public protocol SchemaIntrospector: Sendable {
    /// Retrieve full metadata for a table (columns, constraints, indexes, triggers).
    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata

    /// Retrieve view metadata (definition + columns).
    func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata

    /// Retrieve materialized view metadata.
    func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata

    /// Retrieve sequence metadata.
    func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata

    /// Retrieve enum metadata (labels).
    func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata

    /// Retrieve function or procedure metadata.
    func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata

    /// Retrieve schema metadata.
    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata

    /// Retrieve role metadata.
    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata

    /// Retrieve extension metadata.
    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata

    /// Approximate size in bytes (pg_total_relation_size). Nil for non-relation objects.
    func relationSize(_ id: ObjectIdentifier) async throws -> Int?

    /// List all objects matching optional filters.
    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier]

    /// Permissions (role, privilege pairs) for an object.
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant]

    /// Resolve dependencies for an object via pg_depend.
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier]
}
