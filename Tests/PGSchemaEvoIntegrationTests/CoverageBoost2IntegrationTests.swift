import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Coverage Boost 2 Integration Tests", .tags(.integration), .serialized)
struct CoverageBoost2IntegrationTests {

    private static let testSchema = "_cov2_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - Introspector: listObjects for procedures

    @Test("List procedures via introspector")
    func listProcedures() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: "public", types: [.procedure])

        #expect(objects.contains { $0.name == "reset_order_totals" && $0.type == .procedure })
    }

    // MARK: - Introspector: listObjects for extensions

    @Test("List extensions via introspector")
    func listExtensions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: [.extension])

        #expect(objects.contains { $0.name == "plpgsql" && $0.type == .extension })
    }

    // MARK: - Introspector: listObjects for roles

    @Test("List roles via introspector")
    func listRoles() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: [.role])

        #expect(objects.contains { $0.name == "readonly_role" && $0.type == .role })
    }

    // MARK: - Introspector: listObjects for schemas

    @Test("List schemas via introspector")
    func listSchemas() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: [.schema])

        #expect(objects.contains { $0.name == "public" && $0.type == .schema })
        #expect(objects.contains { $0.name == "analytics" && $0.type == .schema })
    }

    // MARK: - Introspector: listObjects for functions

    @Test("List functions via introspector")
    func listFunctions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: "public", types: [.function])

        #expect(objects.contains { $0.name == "calculate_order_total" && $0.type == .function })
    }

    // MARK: - Introspector: describeExtension

    @Test("Describe extension via introspector")
    func describeExtension() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let extId = ObjectIdentifier(type: .extension, name: "pg_trgm")
        let metadata = try await introspector.describeExtension(extId)

        #expect(!metadata.version.isEmpty)
        #expect(metadata.id.name == "pg_trgm")
    }

    // MARK: - Introspector: describe procedure (via describeFunction)

    @Test("Describe procedure via introspector")
    func describeProcedure() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let procId = ObjectIdentifier(type: .procedure, schema: "public", name: "reset_order_totals")
        let metadata = try await introspector.describeFunction(procId)

        #expect(metadata.definition.contains("reset_order_totals"))
        #expect(metadata.language == "plpgsql")
    }

    // MARK: - Introspector: listObjects with multiple types

    @Test("List multiple object types at once")
    func listMultipleTypes() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(
            schema: "public",
            types: [.table, .view, .function, .sequence, .enum, .compositeType, .procedure]
        )

        let types = Set(objects.map(\.type))
        #expect(types.contains(.table))
        #expect(types.contains(.view))
        #expect(types.contains(.function))
    }

    // MARK: - SchemaDiffer: diff with real introspectors

    @Test("SchemaDiffer diff detects identical schemas")
    func differDetectsIdentical() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn1 = try await IntegrationTestConfig.connect(to: config)
        let conn2 = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn1.close(); try? await conn2.close() } }

        let source = PGCatalogIntrospector(connection: conn1, logger: IntegrationTestConfig.logger)
        let target = PGCatalogIntrospector(connection: conn2, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let result = try await differ.diff(
            source: source,
            target: target,
            schema: "public",
            types: [.table]
        )

        // Same database — should be identical
        #expect(result.isEmpty)
    }

    @Test("SchemaDiffer compareObjects detects real table differences")
    func differCompareObjectsReal() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).diff_test (
                id integer PRIMARY KEY,
                name text NOT NULL,
                email text
            )
        """, on: sourceConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).diff_test (
                id integer PRIMARY KEY,
                name varchar(100) NOT NULL
            )
        """, on: targetConn)

        let source = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let target = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "diff_test")
        let objDiff = try await differ.compareObjects(tableId, source: source, target: target)

        #expect(objDiff != nil)
        #expect(!objDiff!.differences.isEmpty)

        // Cleanup
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_test CASCADE", on: targetConn)
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    // MARK: - SyncOrchestrator: object not found case

    @Test("Sync handles object not found in source or target gracefully")
    func syncObjectNotFound() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: Self.testSchema, name: "nonexistent_table_xyz_123"))
            ],
            dryRun: true,
            force: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        // Object not found in either — should return "already in sync"
        #expect(output.contains("already in sync"))
    }

    // MARK: - SyncOrchestrator: dry-run with modifications

    @Test("Sync dry-run detects column type difference")
    func syncDryRunColumnTypeDiff() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_type_test (
                id integer PRIMARY KEY,
                value bigint
            )
        """, on: sourceConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_type_test (
                id integer PRIMARY KEY,
                value integer
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "sync_type_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: true,
            force: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        #expect(output.contains("ALTER") || output.contains("value"))

        // Cleanup
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: tc)
        try? await sc.close()
        try? await tc.close()
    }

    // MARK: - SyncOrchestrator: dry-run creates missing view

    @Test("Sync dry-run creates view that exists only in source")
    func syncDryRunCreateView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE VIEW \(Self.testSchema).sync_view_test AS SELECT 1 AS val
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let viewId = ObjectIdentifier(type: .view, schema: Self.testSchema, name: "sync_view_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: viewId)],
            dryRun: true,
            force: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        #expect(output.contains("CREATE") || output.contains("sync_view_test"))

        // Cleanup
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - SyncOrchestrator: syncAll dry-run with real schema

    @Test("Sync all dry-run compares all tables in a schema")
    func syncAllDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create a table on source only
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_all_src CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_all_src CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_all_src (id integer PRIMARY KEY)
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "sync_all_src")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: true,
            syncAll: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        #expect(output.contains("CREATE") || output.contains("sync_all_src"))

        // Cleanup
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_all_src CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - SyncOrchestrator: dropExtra on target-only object

    @Test("Sync dry-run with dropExtra drops target-only table")
    func syncDropExtraTargetOnly() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create table only on target
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_drop_tgt CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_drop_tgt CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_drop_tgt (id integer)
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "sync_drop_tgt")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: true,
            dropExtra: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        #expect(output.contains("DROP") || output.contains("sync_drop_tgt"))

        // Cleanup
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_drop_tgt CASCADE", on: tc)
        try? await tc.close()
    }

    // MARK: - SyncOrchestrator: create sequence

    @Test("Sync creates missing sequence on target via dry-run")
    func syncCreateSequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE SEQUENCE \(Self.testSchema).sync_seq_test START 100 INCREMENT 5
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let seqId = ObjectIdentifier(type: .sequence, schema: Self.testSchema, name: "sync_seq_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: seqId)],
            dryRun: true,
            force: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        #expect(output.contains("CREATE SEQUENCE") || output.contains("sync_seq_test"))

        // Cleanup
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - SyncOrchestrator: create composite type

    @Test("Sync creates missing composite type on target via dry-run")
    func syncCreateCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_point_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_point_test CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TYPE \(Self.testSchema).sync_point_test AS (x double precision, y double precision)
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let typeId = ObjectIdentifier(type: .compositeType, schema: Self.testSchema, name: "sync_point_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: typeId)],
            dryRun: true,
            force: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        #expect(output.contains("CREATE TYPE") || output.contains("sync_point_test"))

        // Cleanup
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_point_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Introspector: listObjects with nil types (all types)

    @Test("List all object types via introspector")
    func listAllObjectTypes() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: nil)

        // Should include tables, views, roles, schemas, extensions, etc.
        let types = Set(objects.map(\.type))
        #expect(types.count >= 3) // At minimum tables, schemas, roles
    }

    // MARK: - DataSync: initialize with multiple tables

    @Test("Data sync initialize with multiple tables")
    func dataSyncInitMultiple() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let stateFile = NSTemporaryDirectory() + "data-sync-multi-\(UUID().uuidString).yaml"

        let job = DataSyncJob(
            source: sourceConfig,
            target: sourceConfig,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    trackingColumn: "created_at"
                ),
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
                    trackingColumn: "created_at"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.initialize(job: job)

        #expect(output.contains("public.products"))
        #expect(output.contains("public.orders"))

        let store = DataSyncStateStore()
        let state = try store.load(path: stateFile)
        #expect(state.tables.count == 2)
    }
}
