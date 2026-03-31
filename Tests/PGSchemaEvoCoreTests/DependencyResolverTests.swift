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

// MARK: - Cascade Dependency Tests

@Suite("DependencyResolver Cascade Tests")
struct DependencyResolverCascadeTests {
    let resolver = DependencyResolver()

    @Test("Cascade resolves simple dependency chain")
    func cascadeSimpleChain() async throws {
        // A depends on B; cascade from A should return [B, A]
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "b")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [b],
            b: [],
        ])

        let specs = [ObjectSpec(id: a)]
        let result = try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)

        let names = result.map(\.id.name)
        #expect(names == ["b", "a"])
    }

    @Test("Cascade resolves transitive dependencies")
    func cascadeTransitive() async throws {
        // A -> B -> C
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let c = ObjectIdentifier(type: .table, schema: "public", name: "c")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [b],
            b: [c],
            c: [],
        ])

        let specs = [ObjectSpec(id: a)]
        let result = try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)

        let names = result.map(\.id.name)
        #expect(names == ["c", "b", "a"])
    }

    @Test("Cascade resolves diamond dependencies")
    func cascadeDiamond() async throws {
        // A -> B, A -> C, B -> D, C -> D
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let c = ObjectIdentifier(type: .table, schema: "public", name: "c")
        let d = ObjectIdentifier(type: .table, schema: "public", name: "d")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [b, c],
            b: [d],
            c: [d],
            d: [],
        ])

        let specs = [ObjectSpec(id: a)]
        let result = try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)

        let names = result.map(\.id.name)
        // D must come first, A must come last
        #expect(names.first == "d")
        #expect(names.last == "a")
        // B and C must both appear before A
        let aIndex = names.firstIndex(of: "a")!
        let bIndex = names.firstIndex(of: "b")!
        let cIndex = names.firstIndex(of: "c")!
        let dIndex = names.firstIndex(of: "d")!
        #expect(dIndex < bIndex)
        #expect(dIndex < cIndex)
        #expect(bIndex < aIndex)
        #expect(cIndex < aIndex)
    }

    @Test("Cycle detection throws dependencyCycle error")
    func cycleDetection() async throws {
        // A -> B -> A (cycle)
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "b")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [b],
            b: [a],
        ])

        let specs = [ObjectSpec(id: a)]
        await #expect(throws: PGSchemaEvoError.self) {
            try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)
        }
    }

    @Test("Cascade preserves original spec properties")
    func cascadePreservesProperties() async throws {
        // A depends on B; A has copyData=true, discovered B should get copyData=false
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "b")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [b],
            b: [],
        ])

        let specs = [ObjectSpec(id: a, copyData: true)]
        let result = try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)

        let specA = result.first { $0.id.name == "a" }!
        let specB = result.first { $0.id.name == "b" }!
        #expect(specA.copyData == true)
        #expect(specB.copyData == false)
    }

    @Test("All 15 object types are sorted in correct creation order")
    func allObjectTypesOrder() async throws {
        let specs: [ObjectSpec] = [
            ObjectSpec(id: ObjectIdentifier(type: .operator, schema: "public", name: "op1")),
            ObjectSpec(id: ObjectIdentifier(type: .aggregate, schema: "public", name: "agg1")),
            ObjectSpec(id: ObjectIdentifier(type: .procedure, schema: "public", name: "proc1")),
            ObjectSpec(id: ObjectIdentifier(type: .function, schema: "public", name: "fn1")),
            ObjectSpec(id: ObjectIdentifier(type: .materializedView, schema: "public", name: "mv1")),
            ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "v1")),
            ObjectSpec(id: ObjectIdentifier(type: .foreignTable, schema: "public", name: "ft1")),
            ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t1")),
            ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "seq1")),
            ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: "public", name: "ct1")),
            ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "e1")),
            ObjectSpec(id: ObjectIdentifier(type: .foreignDataWrapper, name: "fdw1")),
            ObjectSpec(id: ObjectIdentifier(type: .extension, name: "ext1")),
            ObjectSpec(id: ObjectIdentifier(type: .schema, name: "s1")),
            ObjectSpec(id: ObjectIdentifier(type: .role, name: "r1")),
        ]

        let result = try await resolver.resolve(
            objects: specs,
            introspector: MockIntrospector(),
            cascade: false
        )

        let expectedOrder: [ObjectType] = [
            .role, .schema, .extension, .foreignDataWrapper, .enum, .compositeType,
            .sequence, .table, .foreignTable, .view, .materializedView, .function,
            .procedure, .aggregate, .operator,
        ]
        let types = result.map(\.id.type)
        #expect(types == expectedOrder)
    }

    @Test("Cascade with no dependencies returns single object")
    func cascadeNoDeps() async throws {
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [],
        ])

        let specs = [ObjectSpec(id: a)]
        let result = try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)

        #expect(result.count == 1)
        #expect(result[0].id.name == "a")
    }

    @Test("Multiple root objects with shared dependency")
    func multipleRootsSharedDep() async throws {
        // A -> C, B -> C; result should have C first, then A and B
        let a = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let c = ObjectIdentifier(type: .table, schema: "public", name: "c")

        let introspector = ConfigurableMockIntrospector(dependencyMap: [
            a: [c],
            b: [c],
            c: [],
        ])

        let specs = [ObjectSpec(id: a), ObjectSpec(id: b)]
        let result = try await resolver.resolve(objects: specs, introspector: introspector, cascade: true)

        let names = result.map(\.id.name)
        #expect(result.count == 3)
        // C must come before both A and B
        let cIndex = names.firstIndex(of: "c")!
        let aIndex = names.firstIndex(of: "a")!
        let bIndex = names.firstIndex(of: "b")!
        #expect(cIndex < aIndex)
        #expect(cIndex < bIndex)
    }
}

// MARK: - Mock Introspectors

/// Configurable mock that returns specified dependencies for each object.
private final class ConfigurableMockIntrospector: SchemaIntrospector, @unchecked Sendable {
    let dependencyMap: [ObjectIdentifier: [ObjectIdentifier]]

    init(dependencyMap: [ObjectIdentifier: [ObjectIdentifier]]) {
        self.dependencyMap = dependencyMap
    }

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
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] {
        dependencyMap[id] ?? []
    }
    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo { RLSInfo() }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
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
    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo { RLSInfo() }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
}
