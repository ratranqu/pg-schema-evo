import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Advanced Sync Integration Tests", .tags(.integration), .serialized)
struct AdvancedSyncIntegrationTests {

    private static let testSchema = "_adv_sync_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - Sync with modified objects (ALTER path)

    @Test("Sync detects and applies column addition")
    func syncColumnAddition() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Create test tables with different schemas
        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_col_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_col_test CASCADE", on: targetConn)

        // Source has extra column
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_col_test (
                id integer PRIMARY KEY,
                name text NOT NULL,
                email text
            )
        """, on: sourceConn)

        // Target is missing the email column
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_col_test (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "sync_col_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify column was added
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'sync_col_test' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(columns.contains("email"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_col_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_col_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sync with syncAll mode

    @Test("Sync with syncAll detects all new objects in schema")
    func syncAllNewObjects() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create a sequence on source that doesn't exist on target
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_all_seq CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_all_seq CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE SEQUENCE \(Self.testSchema).sync_all_seq START 100", on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let seqId = ObjectIdentifier(type: .sequence, schema: Self.testSchema, name: "sync_all_seq")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: seqId)],
            dryRun: false,
            force: true,
            syncAll: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify sequence was created on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'sync_all_seq' AND n.nspname = '\(Self.testSchema)' AND c.relkind = 'S'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_all_seq CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_all_seq CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sync dry-run with modifications

    @Test("Sync dry-run of modified table returns ALTER script")
    func syncDryRunModifiedTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_mod_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_mod_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_mod_test (
                id integer PRIMARY KEY,
                name text NOT NULL,
                active boolean DEFAULT true
            )
        """, on: sourceConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_mod_test (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "sync_mod_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await syncOrchestrator.execute(job: syncJob)

        #expect(output.contains("ALTER TABLE") || output.contains("ADD COLUMN"))

        // Clean up
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_mod_test CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_mod_test CASCADE", on: tc)
        try? await sc.close()
        try? await tc.close()
    }

    // MARK: - Sync with object not found

    @Test("Sync handles object not in source or target gracefully")
    func syncObjectNotFound() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let nonexistentId = ObjectIdentifier(type: .sequence, schema: "public", name: "nonexistent_sync_seq_xyz")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: nonexistentId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await syncOrchestrator.execute(job: syncJob)

        // Should report already in sync (no changes needed since object doesn't exist)
        #expect(result.contains("already in sync"))
    }

    // MARK: - Sync table creation

    @Test("Sync creates missing table on target")
    func syncCreatesMissingTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_create_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_create_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_create_test (
                id integer PRIMARY KEY,
                value text
            )
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "sync_create_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify table was created on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.tables WHERE table_schema = '\(Self.testSchema)' AND table_name = 'sync_create_test'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_create_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_create_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sync view creation

    @Test("Sync creates missing view on target")
    func syncCreatesMissingView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_view_base CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_view_base CASCADE", on: targetConn)

        // Create base table on both sides
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_view_base (id integer PRIMARY KEY, name text)
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).sync_view_base (id integer PRIMARY KEY, name text)
        """, on: targetConn)

        // Create view only on source
        try await IntegrationTestConfig.execute("""
            CREATE VIEW \(Self.testSchema).sync_view_test AS SELECT id, name FROM \(Self.testSchema).sync_view_base
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let viewId = ObjectIdentifier(type: .view, schema: Self.testSchema, name: "sync_view_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: viewId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify view was created
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.views WHERE table_schema = '\(Self.testSchema)' AND table_name = 'sync_view_test'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: vc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_view_base CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).sync_view_test CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).sync_view_base CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sync composite type creation

    @Test("Sync creates missing composite type on target")
    func syncCreatesMissingCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TYPE \(Self.testSchema).sync_type_test AS (x integer, y integer, label text)
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let typeId = ObjectIdentifier(type: .compositeType, schema: Self.testSchema, name: "sync_type_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: typeId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify type was created
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid WHERE t.typname = 'sync_type_test' AND n.nspname = '\(Self.testSchema)' AND t.typtype = 'c'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_type_test CASCADE", on: sc)
        try? await sc.close()
    }
}
