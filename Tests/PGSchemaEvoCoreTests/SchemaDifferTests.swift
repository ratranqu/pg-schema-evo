import Testing
import Logging
@testable import PGSchemaEvoCore

@Suite("SchemaDiffer Tests")
struct SchemaDifferTests {

    @Test("Empty diff when schemas are identical")
    func identicalSchemas() async throws {
        let mock = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "orders"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: mock, target: mock)

        #expect(result.isEmpty)
        #expect(result.renderText().contains("identical"))
    }

    @Test("Detects objects only in source")
    func onlyInSource() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "orders"),
        ])
        let target = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.onlyInSource.count == 1)
        #expect(result.onlyInSource[0].name == "orders")
        #expect(result.onlyInTarget.isEmpty)
    }

    @Test("Detects objects only in target")
    func onlyInTarget() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
        ])
        let target = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "legacy"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.onlyInTarget.count == 1)
        #expect(result.onlyInTarget[0].name == "legacy")
    }

    @Test("Text diff format includes summary")
    func textDiffFormat() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "new_table"),
        ])
        let target = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let text = result.renderText()
        #expect(text.contains("only in source"))
        #expect(text.contains("Summary:"))
    }

    @Test("SQL migration output includes BEGIN/COMMIT")
    func migrationSQLFormat() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "new_table"),
        ])
        let target = MockDiffIntrospector(objects: [])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sql = result.renderMigrationSQL()
        #expect(sql.contains("BEGIN;"))
        #expect(sql.contains("COMMIT;"))
    }
}

/// Mock introspector for diff tests that returns canned object lists.
private final class MockDiffIntrospector: SchemaIntrospector, @unchecked Sendable {
    let objects: [ObjectIdentifier]

    init(objects: [ObjectIdentifier]) {
        self.objects = objects
    }

    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        var result = objects
        if let schema {
            result = result.filter { $0.schema == schema }
        }
        if let types {
            result = result.filter { types.contains($0.type) }
        }
        return result
    }

    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        TableMetadata(id: id, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
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
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] { [] }
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] { [] }
    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo { RLSInfo() }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
}
