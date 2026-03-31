import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Data Sync Integration Tests", .tags(.integration), .serialized)
struct DataSyncIntegrationTests {

    @Test("Primary key columns introspection")
    func primaryKeyColumns() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let pkCols = try await introspector.primaryKeyColumns(
            for: ObjectIdentifier(type: .table, schema: "public", name: "users")
        )
        try await sourceConn.close()

        #expect(pkCols == ["id"])
    }

    @Test("Primary key columns returns empty for partitioned table without PK")
    func primaryKeyColumnsPartitioned() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let pkCols = try await introspector.primaryKeyColumns(
            for: ObjectIdentifier(type: .table, schema: "public", name: "events")
        )
        try await sourceConn.close()

        #expect(pkCols.isEmpty)
    }

    @Test("Data sync init captures MAX tracking value")
    func dataSyncInit() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let stateFile = NSTemporaryDirectory() + "data-sync-test-\(UUID().uuidString).yaml"

        let job = DataSyncJob(
            source: sourceConfig,
            target: sourceConfig,
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

        // Setup: create test table on source via psql
        let shell = ShellRunner()
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(command: "psql", exitCode: -1, stderr: "psql not found")
        }
        let sourceDSN = sourceConfig.toDSN()
        let sourceEnv = sourceConfig.environment()

        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", """
                DROP TABLE IF EXISTS public.data_sync_test CASCADE;
                CREATE TABLE public.data_sync_test (
                    id integer PRIMARY KEY,
                    value text,
                    updated_at timestamp with time zone NOT NULL DEFAULT now()
                );
                INSERT INTO public.data_sync_test (id, value, updated_at) VALUES
                    (1, 'hello', '2026-03-01 00:00:00+00'),
                    (2, 'world', '2026-03-02 00:00:00+00');
                """],
            environment: sourceEnv
        )

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

        // Cleanup
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", "DROP TABLE IF EXISTS public.data_sync_test CASCADE;"],
            environment: sourceEnv
        )
    }

    @Test("Data sync run upserts new and modified rows")
    func dataSyncRunUpsert() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let shell = ShellRunner()
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(command: "psql", exitCode: -1, stderr: "psql not found")
        }
        let sourceDSN = sourceConfig.toDSN()
        let targetDSN = targetConfig.toDSN()
        let sourceEnv = sourceConfig.environment()
        let targetEnv = targetConfig.environment()

        let ddl = """
            DROP TABLE IF EXISTS public.sync_upsert_test CASCADE;
            CREATE TABLE public.sync_upsert_test (
                id integer PRIMARY KEY,
                value text,
                updated_at timestamp with time zone NOT NULL DEFAULT now()
            );
            """
        _ = try await shell.run(command: psqlPath, arguments: [sourceDSN, "-c", ddl], environment: sourceEnv)
        _ = try await shell.run(command: psqlPath, arguments: [targetDSN, "-c", ddl], environment: targetEnv)

        // Insert initial data on both
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", "INSERT INTO public.sync_upsert_test (id, value, updated_at) VALUES (1, 'original', '2026-03-01 00:00:00+00');"],
            environment: sourceEnv
        )
        _ = try await shell.run(
            command: psqlPath,
            arguments: [targetDSN, "-c", "INSERT INTO public.sync_upsert_test (id, value, updated_at) VALUES (1, 'original', '2026-03-01 00:00:00+00');"],
            environment: targetEnv
        )

        // Write state to "before" the first row
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

        // Update source: modify existing row and add new row
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", """
                UPDATE public.sync_upsert_test SET value = 'updated', updated_at = '2026-03-15 00:00:00+00' WHERE id = 1;
                INSERT INTO public.sync_upsert_test (id, value, updated_at) VALUES (2, 'new_row', '2026-03-15 00:00:00+00');
                """],
            environment: sourceEnv
        )

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

        // Verify target via psql query
        let verifyResult = try await shell.run(
            command: psqlPath,
            arguments: [targetDSN, "-t", "-A", "-c", "SELECT id, value FROM public.sync_upsert_test ORDER BY id;"],
            environment: targetEnv
        )
        let lines = verifyResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\n")
        #expect(lines.count == 2)
        #expect(lines[0].contains("updated"))
        #expect(lines[1].contains("new_row"))

        // Cleanup
        _ = try await shell.run(command: psqlPath, arguments: [sourceDSN, "-c", "DROP TABLE IF EXISTS public.sync_upsert_test CASCADE;"], environment: sourceEnv)
        _ = try await shell.run(command: psqlPath, arguments: [targetDSN, "-c", "DROP TABLE IF EXISTS public.sync_upsert_test CASCADE;"], environment: targetEnv)
    }

    @Test("Data sync dry-run produces script without executing")
    func dataSyncDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let shell = ShellRunner()
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(command: "psql", exitCode: -1, stderr: "psql not found")
        }
        let sourceDSN = sourceConfig.toDSN()
        let sourceEnv = sourceConfig.environment()

        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", """
                DROP TABLE IF EXISTS public.dry_run_test CASCADE;
                CREATE TABLE public.dry_run_test (
                    id integer PRIMARY KEY,
                    value text,
                    seq_id integer NOT NULL DEFAULT 0
                );
                INSERT INTO public.dry_run_test (id, value, seq_id) VALUES (1, 'test', 10);
                """],
            environment: sourceEnv
        )

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

        // Cleanup
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", "DROP TABLE IF EXISTS public.dry_run_test CASCADE;"],
            environment: sourceEnv
        )
    }
}
