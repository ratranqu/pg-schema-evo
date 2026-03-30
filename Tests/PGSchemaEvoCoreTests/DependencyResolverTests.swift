import Testing
@testable import PGSchemaEvoCore

@Suite("DependencyResolver Tests")
struct DependencyResolverTests {
    let resolver = DependencyResolver()

    @Test("Non-cascade returns objects sorted by type order")
    func nonCascadeSorting() async throws {
        let specs = [
            ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "users")),
            ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "status")),
            ObjectSpec(id: ObjectIdentifier(type: .schema, name: "analytics")),
        ]

        let result = try await resolver.resolve(
            objects: specs,
            introspector: MockIntrospector(),
            cascade: false
        )

        // Schema should come before enum, enum before table
        let types = result.map(\.id.type)
        #expect(types == [.schema, .enum, .table])
    }

    @Test("Schemas ordered before enums before tables")
    func typeOrderVerification() async throws {
        let specs = [
            ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "v1")),
            ObjectSpec(id: ObjectIdentifier(type: .role, name: "r1")),
            ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "s1")),
            ObjectSpec(id: ObjectIdentifier(type: .extension, name: "e1")),
        ]

        let result = try await resolver.resolve(
            objects: specs,
            introspector: MockIntrospector(),
            cascade: false
        )

        let types = result.map(\.id.type)
        // role < extension < sequence < view
        #expect(types == [.role, .extension, .sequence, .view])
    }
}

// Minimal mock introspector for testing dependency resolution without a database
private final class MockIntrospector: SchemaIntrospector, @unchecked Sendable {
    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func relationSize(_ id: ObjectIdentifier) async throws -> Int? { nil }
    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] { [] }
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] { [] }
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] { [] }
}
