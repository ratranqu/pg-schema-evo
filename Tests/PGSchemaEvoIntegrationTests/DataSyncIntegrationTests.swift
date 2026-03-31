import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Data Sync Integration Tests", .tags(.integration), .serialized)
struct DataSyncIntegrationTests {

    @Test("Primary key columns introspection")
    func primaryKeyColumns() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let pkCols = try await introspector.primaryKeyColumns(
            for: ObjectIdentifier(type: .table, schema: "public", name: "users")
        )

        #expect(pkCols == ["id"])
    }

    @Test("Primary key columns returns empty for partitioned table without PK")
    func primaryKeyColumnsPartitioned() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let pkCols = try await introspector.primaryKeyColumns(
            for: ObjectIdentifier(type: .table, schema: "public", name: "events")
        )

        // The events table is partitioned and has no explicit PK
        #expect(pkCols.isEmpty)
    }

    @Test("Data sync init captures MAX tracking value")
    func dataSyncInit() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"

        let job = DataSyncJob(
            source: sourceConfig,
            target: sourceConfig, // not used for init
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    trackingColumn: "created_at"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.initialize(job: job)

        #expect(output.contains("public.products"))
        #expect(output.contains("created_at"))

        // Verify state file was written
        let store = DataSyncStateStore()
        let state = try store.load(path: stateFile)
        #expect(state.tables.count == 1)
        #expect(state.tables["public.products"] != nil)
        #expect(state.tables["public.products"]?.column == "created_at")
        #expect(!state.tables["public.products"]!.lastValue.isEmpty)
    }

    @Test("Data sync init rejects table without PK")
    func dataSyncInitNoPK() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"

        let job = DataSyncJob(
            source: sourceConfig,
            target: sourceConfig,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "events"),
                    trackingColumn: "created_at"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        await #expect(throws: PGSchemaEvoError.self) {
            try await orchestrator.initialize(job: job)
        }
    }

    @Test("Data sync init rejects invalid tracking column")
    func dataSyncInitBadColumn() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"

        let job = DataSyncJob(
            source: sourceConfig,
            target: sourceConfig,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    trackingColumn: "nonexistent_column"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        await #expect(throws: PGSchemaEvoError.self) {
            try await orchestrator.initialize(job: job)
        }
    }

    @Test("Data sync run with no changes reports no changes")
    func dataSyncRunNoChanges() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        // Set up target with the same table
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("""
            DROP TABLE IF EXISTS public.data_sync_test CASCADE;
            CREATE TABLE public.data_sync_test (
                id integer PRIMARY KEY,
                value text,
                updated_at timestamp with time zone NOT NULL DEFAULT now()
            );
            """, on: targetConn)

        // Create source table with data
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("""
            DROP TABLE IF EXISTS public.data_sync_test CASCADE;
            CREATE TABLE public.data_sync_test (
                id integer PRIMARY KEY,
                value text,
                updated_at timestamp with time zone NOT NULL DEFAULT now()
            );
            INSERT INTO public.data_sync_test (id, value, updated_at) VALUES
                (1, 'hello', '2026-03-01 00:00:00+00'),
                (2, 'world', '2026-03-02 00:00:00+00');
            """, on: sourceConn)

        defer {
            Task {
                try? await IntegrationTestConfig.execute(
                    "DROP TABLE IF EXISTS public.data_sync_test CASCADE", on: sourceConn)
                try? await IntegrationTestConfig.execute(
                    "DROP TABLE IF EXISTS public.data_sync_test CASCADE", on: targetConn)
                try? await sourceConn.close()
                try? await targetConn.close()
            }
        }

        // Init state with current max
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"
        let initJob = DataSyncJob(
            source: sourceConfig,
            target: targetConfig,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "data_sync_test"),
                    trackingColumn: "updated_at"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.initialize(job: initJob)

        // Run sync — no new data since init
        let runJob = DataSyncJob(
            source: sourceConfig,
            target: targetConfig,
            tables: [],
            stateFilePath: stateFile,
            force: true
        )

        let output = try await orchestrator.run(job: runJob)
        #expect(output.contains("no changes"))
    }

    @Test("Data sync run upserts new and modified rows")
    func dataSyncRunUpsert() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Create identical tables on both
        let ddl = """
            DROP TABLE IF EXISTS public.sync_upsert_test CASCADE;
            CREATE TABLE public.sync_upsert_test (
                id integer PRIMARY KEY,
                value text,
                updated_at timestamp with time zone NOT NULL DEFAULT now()
            );
            """
        try await IntegrationTestConfig.execute(ddl, on: sourceConn)
        try await IntegrationTestConfig.execute(ddl, on: targetConn)

        // Insert initial data on source
        try await IntegrationTestConfig.execute("""
            INSERT INTO public.sync_upsert_test (id, value, updated_at) VALUES
                (1, 'original', '2026-03-01 00:00:00+00');
            """, on: sourceConn)

        // Also insert on target (same row)
        try await IntegrationTestConfig.execute("""
            INSERT INTO public.sync_upsert_test (id, value, updated_at) VALUES
                (1, 'original', '2026-03-01 00:00:00+00');
            """, on: targetConn)

        defer {
            Task {
                try? await IntegrationTestConfig.execute(
                    "DROP TABLE IF EXISTS public.sync_upsert_test CASCADE", on: sourceConn)
                try? await IntegrationTestConfig.execute(
                    "DROP TABLE IF EXISTS public.sync_upsert_test CASCADE", on: targetConn)
                try? await sourceConn.close()
                try? await targetConn.close()
            }
        }

        // Init state to "before" the first row
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"
        let stateStore = DataSyncStateStore()
        try stateStore.save(
            state: DataSyncState(tables: [
                "public.sync_upsert_test": DataSyncTableState(
                    column: "updated_at",
                    lastValue: "2026-02-28 00:00:00+00"
                ),
            ]),
            path: stateFile
        )

        // Now update the row on source and add a new row
        try await IntegrationTestConfig.execute("""
            UPDATE public.sync_upsert_test SET value = 'updated', updated_at = '2026-03-15 00:00:00+00' WHERE id = 1;
            INSERT INTO public.sync_upsert_test (id, value, updated_at) VALUES
                (2, 'new_row', '2026-03-15 00:00:00+00');
            """, on: sourceConn)

        // Run sync
        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        let runJob = DataSyncJob(
            source: sourceConfig,
            target: targetConfig,
            tables: [],
            stateFilePath: stateFile,
            force: true
        )

        let output = try await orchestrator.run(job: runJob)
        #expect(output.contains("synced"))

        // Verify target has the updated and new rows
        let rows = try await targetConn.query(
            "SELECT id, value FROM public.sync_upsert_test ORDER BY id",
            logger: IntegrationTestConfig.logger
        )
        var results: [(Int, String)] = []
        for try await row in rows {
            let (id, value) = try row.decode((Int, String).self)
            results.append((id, value))
        }

        #expect(results.count == 2)
        #expect(results[0].0 == 1)
        #expect(results[0].1 == "updated")
        #expect(results[1].0 == 2)
        #expect(results[1].1 == "new_row")
    }

    @Test("Data sync dry-run produces script without executing")
    func dataSyncDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        try await IntegrationTestConfig.execute("""
            DROP TABLE IF EXISTS public.dry_run_test CASCADE;
            CREATE TABLE public.dry_run_test (
                id integer PRIMARY KEY,
                value text,
                seq_id integer NOT NULL DEFAULT 0
            );
            INSERT INTO public.dry_run_test (id, value, seq_id) VALUES (1, 'test', 10);
            """, on: sourceConn)

        defer {
            Task {
                try? await IntegrationTestConfig.execute(
                    "DROP TABLE IF EXISTS public.dry_run_test CASCADE", on: sourceConn)
                try? await sourceConn.close()
            }
        }

        // Write state with seq_id = 0 (so all rows are "new")
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"
        let stateStore = DataSyncStateStore()
        try stateStore.save(
            state: DataSyncState(tables: [
                "public.dry_run_test": DataSyncTableState(column: "seq_id", lastValue: "0"),
            ]),
            path: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        let runJob = DataSyncJob(
            source: sourceConfig,
            target: targetConfig,
            tables: [],
            stateFilePath: stateFile,
            dryRun: true,
            force: true
        )

        let output = try await orchestrator.run(job: runJob)
        #expect(output.contains("Dry-run"))
        #expect(output.contains("1 row(s) to sync"))
    }
}
