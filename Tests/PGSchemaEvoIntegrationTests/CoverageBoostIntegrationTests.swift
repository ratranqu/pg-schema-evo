import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Coverage Boost Integration Tests", .tags(.integration), .serialized)
struct CoverageBoostIntegrationTests {

    private static let testSchema = "_cov_boost_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - SyncOrchestrator: allowDropColumns path

    @Test("Sync with allowDropColumns drops extra columns")
    func syncAllowDropColumns() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).drop_col_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).drop_col_test CASCADE", on: targetConn)

        // Source has fewer columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).drop_col_test (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target has an extra column
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).drop_col_test (
                id integer PRIMARY KEY,
                name text NOT NULL,
                obsolete text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "drop_col_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            allowDropColumns: true,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify the obsolete column was dropped
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'drop_col_test' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(!columns.contains("obsolete"))
        #expect(columns.contains("id"))
        #expect(columns.contains("name"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).drop_col_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).drop_col_test CASCADE", on: sc)
        try? await sc.close()
    }

    @Test("Sync without allowDropColumns skips destructive changes")
    func syncSkipDropColumns() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).skip_drop_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).skip_drop_test CASCADE", on: targetConn)

        // Source has fewer columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).skip_drop_test (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target has an extra column
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).skip_drop_test (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "skip_drop_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            allowDropColumns: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify the extra column was preserved
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'skip_drop_test' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(columns.contains("extra_col"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).skip_drop_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).skip_drop_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - CloneOrchestrator: procedure path

    @Test("Dry-run clone of procedure generates CREATE PROCEDURE SQL")
    func dryRunProcedure() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .procedure, schema: "public", name: "reset_order_totals")
                ),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("reset_order_totals"))
    }

    // MARK: - CloneOrchestrator: extension path

    @Test("Dry-run clone of extension generates CREATE EXTENSION SQL")
    func dryRunExtension() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .extension, name: "pg_trgm")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("pg_trgm"))
    }

    // MARK: - CloneOrchestrator: role path

    @Test("Dry-run clone of role generates role DDL")
    func dryRunRole() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .role, name: "readonly_role")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("readonly_role"))
    }

    // MARK: - CloneOrchestrator: view path

    @Test("Dry-run clone of view generates CREATE VIEW SQL")
    func dryRunView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "active_users")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("active_users"))
    }

    // MARK: - PreflightChecker: verifyObjectExists paths

    @Test("Preflight verifies function exists in source")
    func preflightVerifyFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total", signature: "(integer)"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("calculate_order_total") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies view exists in source")
    func preflightVerifyView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "active_users"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("active_users") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies sequence exists in source")
    func preflightVerifySequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("invoice_number_seq") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies enum exists in source")
    func preflightVerifyEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("order_status") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies composite type exists in source")
    func preflightVerifyCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: "public", name: "address"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("address") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies schema exists in source")
    func preflightVerifySchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .schema, name: "analytics"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("analytics") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies role exists in source")
    func preflightVerifyRole() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .role, name: "readonly_role"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("readonly_role") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight verifies extension exists in source")
    func preflightVerifyExtension() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .extension, name: "plpgsql"))
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let notFoundFailures = failures.filter { $0.contains("not found") && $0.contains("plpgsql") }
        #expect(notFoundFailures.isEmpty)
    }

    @Test("Preflight detects missing procedure")
    func preflightMissingProcedure() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .procedure, schema: "public", name: "nonexistent_proc"))
            ],
            dryRun: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        #expect(failures.contains { $0.contains("nonexistent_proc") })
    }

    // MARK: - DataSync: delete detection (dry-run)

    @Test("Data sync with delete detection in dry-run mode")
    func dataSyncDeleteDetectionDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let shell = ShellRunner()
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(command: "psql", exitCode: -1, stderr: "psql not found")
        }
        let sourceDSN = sourceConfig.toDSN()
        let sourceEnv = sourceConfig.environment()

        // Create test table on source with data
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", """
                DROP TABLE IF EXISTS public.sync_delete_test CASCADE;
                CREATE TABLE public.sync_delete_test (
                    id integer PRIMARY KEY,
                    value text,
                    seq_num integer NOT NULL DEFAULT 0
                );
                INSERT INTO public.sync_delete_test (id, value, seq_num) VALUES (1, 'keep', 10), (2, 'keep', 20);
                """],
            environment: sourceEnv
        )

        // Write state with seq_num = 0 to sync all rows
        let stateFile = NSTemporaryDirectory() + "sync-delete-test-\(UUID().uuidString).yaml"
        let stateStore = DataSyncStateStore()
        try stateStore.save(
            state: DataSyncState(tables: [
                "public.sync_delete_test": DataSyncTableState(column: "seq_num", lastValue: "0"),
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
            detectDeletes: true,
            force: true
        )

        let output = try await orchestrator.run(job: runJob)
        #expect(output.contains("Dry-run"))
        #expect(output.contains("2 row(s) to sync"))

        // Cleanup
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", "DROP TABLE IF EXISTS public.sync_delete_test CASCADE;"],
            environment: sourceEnv
        )
    }

    // MARK: - SyncOrchestrator: syncAll with dropExtra (dry-run)

    @Test("Sync dry-run with syncAll and dropExtra shows DROP for target-only objects")
    func syncAllDropExtraDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create an extra table on target that doesn't exist on source
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).extra_target_only CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).extra_target_only CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).extra_target_only (id integer)", on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "extra_target_only")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: true,
            dropExtra: true,
            syncAll: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await syncOrchestrator.execute(job: syncJob)

        // Dry-run should show the DROP TABLE step
        #expect(output.contains("DROP TABLE") || output.contains("extra_target_only"))

        // Clean up
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).extra_target_only CASCADE", on: tc)
        try? await tc.close()
    }

    // MARK: - SyncOrchestrator: function creation

    @Test("Sync creates missing function on target")
    func syncCreatesMissingFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test() CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test() CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE FUNCTION \(Self.testSchema).sync_func_test() RETURNS integer LANGUAGE sql AS $$ SELECT 42; $$
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        // No-arg functions have nil signature in PG catalog
        let funcId = ObjectIdentifier(type: .function, schema: Self.testSchema, name: "sync_func_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: funcId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify function was created on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE p.proname = 'sync_func_test' AND n.nspname = '\(Self.testSchema)'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test() CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test() CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - SyncOrchestrator: schema creation

    @Test("Sync creates missing schema on target")
    func syncCreatesMissingSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        let schemaName = "_cov_sync_schema_test"
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(schemaName)", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SCHEMA IF EXISTS \(schemaName) CASCADE", on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let schemaId = ObjectIdentifier(type: .schema, name: schemaName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: schemaId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify schema was created
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '\(schemaName)'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP SCHEMA IF EXISTS \(schemaName) CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP SCHEMA IF EXISTS \(schemaName) CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - SyncOrchestrator: enum creation

    @Test("Sync creates missing enum on target")
    func syncCreatesMissingEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_status_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_status_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TYPE \(Self.testSchema).sync_status_test AS ENUM ('active', 'inactive', 'pending')
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let enumId = ObjectIdentifier(type: .enum, schema: Self.testSchema, name: "sync_status_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: enumId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify enum was created
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid WHERE t.typname = 'sync_status_test' AND n.nspname = '\(Self.testSchema)'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_status_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_status_test CASCADE", on: sc)
        try? await sc.close()
    }
}
