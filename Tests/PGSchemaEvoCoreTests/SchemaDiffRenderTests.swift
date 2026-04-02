import Testing
import Logging
@testable import PGSchemaEvoCore

@Suite("SchemaDiff Render Tests")
struct SchemaDiffRenderTests {

    // MARK: - renderText edge cases

    @Test("renderText with all sections populated")
    func renderTextAllSections() {
        let srcOnly = ObjectIdentifier(type: .table, schema: "public", name: "new_table")
        let tgtOnly = ObjectIdentifier(type: .view, schema: "public", name: "old_view")
        let modified = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            differences: ["Column name: type varchar -> text"],
            migrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"name\" TYPE text;"]
        )

        let diff = SchemaDiff(
            onlyInSource: [srcOnly],
            onlyInTarget: [tgtOnly],
            modified: [modified],
            matching: 3
        )

        let text = diff.renderText()
        #expect(text.contains("Objects only in source (1)"))
        #expect(text.contains("+ table:public.new_table"))
        #expect(text.contains("Objects only in target (1)"))
        #expect(text.contains("- view:public.old_view"))
        #expect(text.contains("Modified objects (1)"))
        #expect(text.contains("~ table:public.users"))
        #expect(text.contains("Column name: type varchar -> text"))
        #expect(text.contains("Summary: 3 matching, 1 only in source, 1 only in target, 1 modified"))
    }

    @Test("renderText with empty diff shows identical message")
    func renderTextEmpty() {
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [], matching: 10)
        let text = diff.renderText()
        #expect(text.contains("Schemas are identical"))
    }

    // MARK: - renderMigrationSQL drop statement types

    @Test("renderMigrationSQL generates DROP VIEW for view")
    func dropViewSQL() {
        let viewId = ObjectIdentifier(type: .view, schema: "public", name: "my_view")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [viewId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP VIEW IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP MATERIALIZED VIEW for matview")
    func dropMatviewSQL() {
        let mvId = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [mvId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP MATERIALIZED VIEW IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP SEQUENCE for sequence")
    func dropSequenceSQL() {
        let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "my_seq")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [seqId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP SEQUENCE IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP FUNCTION for function")
    func dropFunctionSQL() {
        let funcId = ObjectIdentifier(type: .function, schema: "public", name: "my_func")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [funcId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP FUNCTION IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP PROCEDURE for procedure")
    func dropProcedureSQL() {
        let procId = ObjectIdentifier(type: .procedure, schema: "public", name: "my_proc")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [procId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP PROCEDURE IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP TYPE for enum")
    func dropEnumSQL() {
        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [enumId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP TYPE IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP TYPE for composite type")
    func dropCompositeTypeSQL() {
        let typeId = ObjectIdentifier(type: .compositeType, schema: "public", name: "addr")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [typeId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP TYPE IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP SCHEMA for schema")
    func dropSchemaSQL() {
        let schemaId = ObjectIdentifier(type: .schema, name: "old_schema")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [schemaId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP SCHEMA IF EXISTS"))
    }

    @Test("renderMigrationSQL generates DROP EXTENSION for extension")
    func dropExtensionSQL() {
        let extId = ObjectIdentifier(type: .extension, name: "pg_trgm")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [extId], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP EXTENSION IF EXISTS"))
    }

    // MARK: - ObjectDiff

    @Test("ObjectDiff default dropColumnSQL is empty")
    func objectDiffDefaults() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let diff = ObjectDiff(id: id, differences: ["a change"], migrationSQL: ["ALTER TABLE t ADD x;"])
        #expect(diff.dropColumnSQL.isEmpty)
    }

    @Test("ObjectDiff with dropColumnSQL")
    func objectDiffWithDrop() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let diff = ObjectDiff(
            id: id,
            differences: ["Column x: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE t DROP COLUMN x;"]
        )
        #expect(diff.dropColumnSQL.count == 1)
    }

    // MARK: - SchemaDiff with only source objects

    @Test("renderMigrationSQL shows source-only objects as comments")
    func sourceOnlyAsComments() {
        let srcOnly = ObjectIdentifier(type: .sequence, schema: "public", name: "new_seq")
        let diff = SchemaDiff(onlyInSource: [srcOnly], onlyInTarget: [], modified: [], matching: 0)
        let sql = diff.renderMigrationSQL()
        #expect(sql.contains("-- Objects missing in target"))
        #expect(sql.contains("-- CREATE sequence:public.new_seq"))
    }

    // MARK: - Role diff additional attributes

    @Test("Role diff detects superuser, createrole, and connection limit changes")
    func roleAllAttrChanges() async throws {
        let roleId = ObjectIdentifier(type: .role, schema: nil, name: "admin")
        let source = RoleDiffIntrospector(objects: [roleId])
        source.roles[roleId] = RoleMetadata(
            id: roleId,
            canLogin: true,
            isSuperuser: true,
            canCreateDB: true,
            canCreateRole: true,
            connectionLimit: 10
        )
        let target = RoleDiffIntrospector(objects: [roleId])
        target.roles[roleId] = RoleMetadata(
            id: roleId,
            canLogin: false,
            isSuperuser: false,
            canCreateDB: false,
            canCreateRole: false,
            connectionLimit: -1
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.count == 5)
        let sql = result.modified[0].migrationSQL[0]
        #expect(sql.contains("SUPERUSER"))
        #expect(sql.contains("CREATEROLE"))
        #expect(sql.contains("CONNECTION LIMIT 10"))
    }

    @Test("Role diff detects NOLOGIN, NOSUPERUSER etc")
    func roleNegativeAttrs() async throws {
        let roleId = ObjectIdentifier(type: .role, schema: nil, name: "limited")
        let source = RoleDiffIntrospector(objects: [roleId])
        source.roles[roleId] = RoleMetadata(id: roleId, canLogin: false, isSuperuser: false)
        let target = RoleDiffIntrospector(objects: [roleId])
        target.roles[roleId] = RoleMetadata(id: roleId, canLogin: true, isSuperuser: true)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        let sql = result.modified[0].migrationSQL[0]
        #expect(sql.contains("NOLOGIN"))
        #expect(sql.contains("NOSUPERUSER"))
    }

    @Test("RLS forced on target but not source generates NO FORCE RLS")
    func rlsNoForceOnTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "data")
        let source = RoleDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: true, isForced: false)
        let target = RoleDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: true, isForced: true)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("NO FORCE ROW LEVEL SECURITY") })
    }

    // MARK: - compareObjects public API

    @Test("compareObjects returns nil when objects are identical")
    func compareObjectsIdentical() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let meta = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
        let introspector = RoleDiffIntrospector(objects: [tableId])
        introspector.tables[tableId] = meta

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.compareObjects(tableId, source: introspector, target: introspector)
        #expect(result == nil)
    }

    @Test("compareObjects returns diff when objects differ")
    func compareObjectsDifferent() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let source = RoleDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "bigint", isNullable: false, ordinalPosition: 1),
        ])
        let target = RoleDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.compareObjects(tableId, source: source, target: target)
        #expect(result != nil)
        #expect(result!.differences.count == 1)
    }
}

// MARK: - Mock introspector supporting all object types

private final class RoleDiffIntrospector: SchemaIntrospector, @unchecked Sendable {
    let objects: [ObjectIdentifier]
    var tables: [ObjectIdentifier: TableMetadata] = [:]
    var views: [ObjectIdentifier: ViewMetadata] = [:]
    var materializedViews: [ObjectIdentifier: MaterializedViewMetadata] = [:]
    var sequences: [ObjectIdentifier: SequenceMetadata] = [:]
    var enums: [ObjectIdentifier: EnumMetadata] = [:]
    var functions: [ObjectIdentifier: FunctionMetadata] = [:]
    var schemas: [ObjectIdentifier: SchemaMetadata] = [:]
    var roles: [ObjectIdentifier: RoleMetadata] = [:]
    var compositeTypes: [ObjectIdentifier: CompositeTypeMetadata] = [:]
    var extensions: [ObjectIdentifier: ExtensionMetadata] = [:]
    var rlsInfos: [ObjectIdentifier: RLSInfo] = [:]

    init(objects: [ObjectIdentifier]) {
        self.objects = objects
    }

    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        var result = objects
        if let schema { result = result.filter { $0.schema == schema } }
        if let types { result = result.filter { types.contains($0.type) } }
        return result
    }

    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        guard let meta = tables[id] else {
            return TableMetadata(id: id, columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ])
        }
        return meta
    }
    func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata {
        guard let meta = views[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata {
        guard let meta = materializedViews[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata {
        guard let meta = sequences[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata {
        guard let meta = enums[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata {
        guard let meta = functions[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        guard let meta = schemas[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        guard let meta = roles[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        guard let meta = compositeTypes[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        guard let meta = extensions[id] else { throw PGSchemaEvoError.objectNotFound(id) }
        return meta
    }
    func relationSize(_ id: ObjectIdentifier) async throws -> Int? { nil }
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] { [] }
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] { [] }
    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo { rlsInfos[id] ?? RLSInfo() }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
    func primaryKeyColumns(for id: ObjectIdentifier) async throws -> [String] { [] }
}
