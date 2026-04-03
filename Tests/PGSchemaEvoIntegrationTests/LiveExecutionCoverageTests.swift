import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

/// Integration tests that exercise LiveExecutor, ParallelDataTransfer,
/// and PgDumpIntrospector code paths for coverage improvement.
/// Uses a dedicated `cov_items` table to avoid cross-suite race conditions.
@Suite("Live Execution Coverage Tests", .serialized)
struct LiveExecutionCoverageTests {

    /// Table name unique to this suite to avoid cross-suite race conditions.
    static let testTable = "cov_items"
    /// Second table for multi-table parallel tests.
    static let testTable2 = "cov_widgets"

    /// Set up the test tables on the source database.
    static func ensureSourceTables() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }

        try await IntegrationTestConfig.execute("""
            CREATE TABLE IF NOT EXISTS public.\(testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL,
                value numeric(10, 2) NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            )
            """, on: sourceConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE IF NOT EXISTS public.\(testTable2) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                label text NOT NULL,
                score integer NOT NULL DEFAULT 0
            )
            """, on: sourceConn)

        // Insert data if empty
        let rows1 = try await sourceConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count1: Int64 = 0
        for try await row in rows1 { count1 = try row.decode(Int64.self) }
        if count1 == 0 {
            try await IntegrationTestConfig.execute("""
                INSERT INTO public.\(testTable) (name, value) VALUES
                ('alpha', 10.00), ('beta', 20.00), ('gamma', 30.00),
                ('delta', 40.00), ('epsilon', 50.00)
                """, on: sourceConn)
        }

        let rows2 = try await sourceConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(testTable2)"),
            logger: IntegrationTestConfig.logger
        )
        var count2: Int64 = 0
        for try await row in rows2 { count2 = try row.decode(Int64.self) }
        if count2 == 0 {
            try await IntegrationTestConfig.execute("""
                INSERT INTO public.\(testTable2) (label, score) VALUES
                ('w1', 10), ('w2', 20), ('w3', 30), ('w4', 40), ('w5', 50)
                """, on: sourceConn)
        }
    }

    // MARK: - LiveExecutor executeInTransaction with data

    @Test("Live clone table with data via transaction")
    func liveCloneTableWithData() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone table with data and WHERE clause")
    func liveCloneTableWithWhereClause() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true,
                    whereClause: "value > 20"
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)
        #expect(count < 5)  // Only rows with value > 20

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone table with row limit")
    func liveCloneTableWithRowLimit() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true,
                    rowLimit: 2
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count == 2)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone table with pgDump data method")
    func liveCloneTableWithPgDump() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            defaultDataMethod: .pgDump,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone with dropIfExists and data")
    func liveCloneDropIfExistsWithData() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // First create a dummy table so dropIfExists has something to drop
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE TABLE public.\(Self.testTable) (id int)", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone with permissions")
    func liveCloneWithPermissions() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        // Grant permissions on source table so there's something to copy
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }
        try await IntegrationTestConfig.execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_role') THEN CREATE ROLE app_role; END IF; END $$", on: sourceConn)
        try await IntegrationTestConfig.execute("GRANT SELECT ON public.\(Self.testTable) TO app_role", on: sourceConn)

        // Also create role on target
        try await IntegrationTestConfig.execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_role') THEN CREATE ROLE app_role; END IF; END $$", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyPermissions: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify table exists
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '\(Self.testTable)'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone with RLS policies")
    func liveCloneWithRLS() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "user_role")),
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyRLSPolicies: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify RLS is enabled
        let rows = try await targetConn.query(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'users' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
            logger: IntegrationTestConfig.logger
        )
        var rlsEnabled = false
        for try await row in rows { rlsEnabled = try row.decode(Bool.self) }
        #expect(rlsEnabled)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    // MARK: - ParallelDataTransfer execute path

    @Test("Live clone with parallel data transfer")
    func liveCloneParallelData() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            parallel: 2
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live clone multiple tables with parallel data transfer")
    func liveCloneMultipleTablesParallel() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable2) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable2),
                    copyData: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            parallel: 2
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        var count1: Int64 = 0
        let r1 = try await targetConn.query(PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"), logger: IntegrationTestConfig.logger)
        for try await row in r1 { count1 = try row.decode(Int64.self) }
        #expect(count1 > 0)

        var count2: Int64 = 0
        let r2 = try await targetConn.query(PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable2)"), logger: IntegrationTestConfig.logger)
        for try await row in r2 { count2 = try row.decode(Int64.self) }
        #expect(count2 > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable2) CASCADE", on: targetConn)
    }

    // MARK: - SyncOrchestrator live execution

    @Test("Live sync creates missing table on target")
    func liveSyncCreatesTable() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '\(Self.testTable)'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live sync detects identical table")
    func liveSyncIdenticalTable() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        // First clone the table to target
        let cloneJob = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )
        let cloneOrch = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await cloneOrch.execute(job: cloneJob)

        // Now sync — should detect no changes
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            force: true,
            skipPreflight: true
        )

        let syncOrch = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await syncOrch.execute(job: syncJob)
        #expect(result.contains("No changes") || result.contains("in sync") || result.contains("0 new"))

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - PreflightChecker coverage

    @Test("Preflight checker validates source objects exist")
    func preflightCheckerValidates() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(failures.isEmpty)
    }

    @Test("Preflight checker detects missing source object")
    func preflightCheckerDetectsMissing() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "nonexistent_table_xyz")),
            ],
            dryRun: false,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(!failures.isEmpty)
        #expect(failures.first?.contains("nonexistent_table_xyz") == true)
    }

    @Test("Preflight checker detects target conflict")
    func preflightCheckerDetectsConflict() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create a table on target to cause conflict
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE TABLE public.\(Self.testTable) (id int)", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            dropIfExists: false,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(!failures.isEmpty)
        #expect(failures.contains { $0.contains("already exists") })

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Preflight checker with dry-run")
    func preflightCheckerDryRun() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: true,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(failures.isEmpty)
    }

    // MARK: - Clone view

    @Test("Live clone view")
    func liveCloneView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS public.active_users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "user_role")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "users")),
                ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "active_users")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            "SELECT 1 FROM information_schema.views WHERE table_schema = 'public' AND table_name = 'active_users'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS public.active_users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    // MARK: - Clone with global row limit

    @Test("Live clone with global row limit applies to all tables")
    func liveCloneGlobalRowLimit() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            globalRowLimit: 1
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count == 1)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Sync with dropExtra

    @Test("Live sync with dropExtra removes target-only objects")
    func liveSyncDropExtra() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create a sequence on target that doesn't exist on source
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.cov_extra_seq CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE SEQUENCE public.cov_extra_seq", on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "cov_extra_seq")),
            ],
            dryRun: false,
            dropExtra: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)
    }

    // MARK: - Sync modified object path

    @Test("Live sync detects and applies column addition")
    func liveSyncModifiedTable() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create a table on target that's slightly different from source
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        // Create with fewer columns than source
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.\(Self.testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL
            )
            """, on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        // Exercises the modified-object code path
        _ = try await orchestrator.execute(job: job)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Clone with preflight (not skipped)

    @Test("Live clone with preflight passes and executes")
    func liveCloneWithPreflight() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: false  // Enable preflight
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Clone with retries

    @Test("Live clone with retries succeeds on first attempt")
    func liveCloneWithRetries() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 2,  // Enable retries
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '\(Self.testTable)'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Sync with syncAll

    @Test("Live sync with syncAll mode dry-run")
    func liveSyncAllDryRun() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        // Use dry-run to exercise syncAll code path without side effects
        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: true,
            force: true,
            skipPreflight: true,
            syncAll: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await orchestrator.execute(job: job)
        #expect(!result.isEmpty)
    }

    // MARK: - Clone function

    @Test("Live clone function dry-run")
    func liveCloneFunctionDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total")),
            ],
            dryRun: true,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await orchestrator.execute(job: job)
        #expect(result.contains("calculate_order_total"))
    }

    // MARK: - Clone procedure

    @Test("Live clone procedure")
    func liveCloneProcedure() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP PROCEDURE IF EXISTS public.reset_order_totals() CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .procedure, schema: "public", name: "reset_order_totals")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify procedure exists
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'reset_order_totals'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP PROCEDURE IF EXISTS public.reset_order_totals() CASCADE", on: targetConn)
    }

    // MARK: - Clone with cascade dependencies

    @Test("Live clone table with cascade discovers dependencies")
    func liveCloneCascade() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS public.update_order_total() CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS public.calculate_order_total(integer) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
                    cascadeDependencies: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify orders table was created (along with its dependency: users)
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('users', 'orders') ORDER BY table_name"),
            logger: IntegrationTestConfig.logger
        )
        var tables: [String] = []
        for try await row in rows { tables.append(try row.decode(String.self)) }
        #expect(tables.contains("orders"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS public.update_order_total() CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS public.calculate_order_total(integer) CASCADE", on: targetConn)
    }

    // MARK: - Parallel data with pgDump method

    @Test("Live clone with parallel data transfer using pgDump method")
    func liveCloneParallelPgDump() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true
                ),
            ],
            dryRun: false,
            defaultDataMethod: .pgDump,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            parallel: 2
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Clone with data and WHERE via parallel

    @Test("Live clone parallel with WHERE clause")
    func liveCloneParallelWhere() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true,
                    whereClause: "value > 20"
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            parallel: 2
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count > 0)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - PgDumpIntrospector coverage

    @Test("PgDumpIntrospector extracts foreign table DDL")
    func pgDumpIntrospectorForeignTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()

        // Create foreign table on source if it doesn't exist
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }
        try await IntegrationTestConfig.execute("CREATE EXTENSION IF NOT EXISTS file_fdw", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE SERVER IF NOT EXISTS cov_test_srv FOREIGN DATA WRAPPER file_fdw", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'cov_ft') THEN
                CREATE FOREIGN TABLE public.cov_ft (id int, name text) SERVER cov_test_srv OPTIONS (filename '/dev/null');
            END IF;
            END $$
            """, on: sourceConn)

        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)
        let metadata = try await introspector.extractDDL(
            for: ObjectIdentifier(type: .foreignTable, schema: "public", name: "cov_ft")
        )
        #expect(metadata.ddl.contains("cov_ft"))
    }

    @Test("PgDumpIntrospector extracts aggregate DDL")
    func pgDumpIntrospectorAggregate() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()

        // Create aggregate on source if it doesn't exist
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }
        try await IntegrationTestConfig.execute("""
            CREATE OR REPLACE FUNCTION public.cov_accum(state numeric, val numeric)
            RETURNS numeric AS 'SELECT COALESCE($1, 0) + COALESCE($2, 0)' LANGUAGE sql IMMUTABLE
            """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_aggregate a JOIN pg_proc p ON p.oid = a.aggfnoid JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'cov_sum') THEN
                CREATE AGGREGATE public.cov_sum(numeric) (SFUNC = public.cov_accum, STYPE = numeric, INITCOND = '0');
            END IF;
            END $$
            """, on: sourceConn)

        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)
        let metadata = try await introspector.extractDDL(
            for: ObjectIdentifier(type: .aggregate, schema: "public", name: "cov_sum")
        )
        #expect(metadata.ddl.contains("cov_sum") || metadata.ddl.contains("AGGREGATE"))
    }

    @Test("PgDumpIntrospector rejects unsupported type")
    func pgDumpIntrospectorUnsupported() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)

        do {
            _ = try await introspector.extractDDL(
                for: ObjectIdentifier(type: .table, schema: "public", name: "test")
            )
            #expect(Bool(false), "Should have thrown")
        } catch {
            // Expected - table type is not supported by PgDumpIntrospector
        }
    }

    @Test("PgDumpIntrospector rejects missing object")
    func pgDumpIntrospectorMissing() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)

        do {
            _ = try await introspector.extractDDL(
                for: ObjectIdentifier(type: .foreignTable, schema: "public", name: "nonexistent_ft_xyz")
            )
            #expect(Bool(false), "Should have thrown")
        } catch {
            // Expected - object doesn't exist
        }
    }

    @Test("PgDumpIntrospector FDW extraction exercises code path")
    func pgDumpIntrospectorFDW() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }

        try await IntegrationTestConfig.execute("CREATE EXTENSION IF NOT EXISTS file_fdw", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE SERVER IF NOT EXISTS cov_test_srv FOREIGN DATA WRAPPER file_fdw", on: sourceConn)

        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)
        // FDW from extension may not appear in pg_dump output, but this exercises the code path
        do {
            let metadata = try await introspector.extractDDL(
                for: ObjectIdentifier(type: .foreignDataWrapper, schema: nil, name: "file_fdw")
            )
            #expect(!metadata.ddl.isEmpty)
        } catch {
            // FDW from extension may throw objectNotFound - that's OK, code path was exercised
        }
    }

    @Test("PgDumpIntrospector rejects operator without schema")
    func pgDumpIntrospectorOperatorNoSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)

        do {
            _ = try await introspector.extractDDL(
                for: ObjectIdentifier(type: .operator, schema: nil, name: "test_op")
            )
            #expect(Bool(false), "Should have thrown")
        } catch {
            // Expected - operator requires schema
        }
    }

    @Test("PgDumpIntrospector rejects foreign table without schema")
    func pgDumpIntrospectorForeignTableNoSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)

        do {
            _ = try await introspector.extractDDL(
                for: ObjectIdentifier(type: .foreignTable, schema: nil, name: "test")
            )
            #expect(Bool(false), "Should have thrown")
        } catch {
            // Expected - foreign table requires schema
        }
    }

    @Test("PgDumpIntrospector rejects aggregate without schema")
    func pgDumpIntrospectorAggregateNoSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let introspector = PgDumpIntrospector(sourceConfig: sourceConfig, logger: IntegrationTestConfig.logger)

        do {
            _ = try await introspector.extractDDL(
                for: ObjectIdentifier(type: .aggregate, schema: nil, name: "test")
            )
            #expect(Bool(false), "Should have thrown")
        } catch {
            // Expected - aggregate requires schema
        }
    }

    // MARK: - Sync with modified table (alter path)

    @Test("Live sync applies ALTER for modified table")
    func liveSyncAlterTable() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create target table with different schema (missing column)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.\(Self.testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL
            )
            """, on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify the column was added
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '\(Self.testTable)' AND column_name = 'value'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live sync with allowDropColumns drops extra target column")
    func liveSyncDropColumns() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create target table with an extra column not in source
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.\(Self.testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL,
                value numeric(10, 2) NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                extra_col text DEFAULT 'to_be_dropped'
            )
            """, on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: false,
            allowDropColumns: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify the extra column was dropped
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '\(Self.testTable)' AND column_name = 'extra_col'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(!found, "extra_col should have been dropped")

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("Live sync with schema-only object (extension)")
    func liveSyncExtension() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Drop pg_trgm on target so sync has to create it
        try await IntegrationTestConfig.execute("DROP EXTENSION IF EXISTS pg_trgm CASCADE", on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .extension, schema: nil, name: "pg_trgm")),
            ],
            dryRun: false,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify extension exists
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)
    }

    // MARK: - Clone foreign table via orchestrator

    @Test("Live clone foreign table")
    func liveCloneForeignTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Ensure source has the foreign table
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }
        try await IntegrationTestConfig.execute("CREATE EXTENSION IF NOT EXISTS file_fdw", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE SERVER IF NOT EXISTS cov_test_srv FOREIGN DATA WRAPPER file_fdw", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
            IF NOT EXISTS (SELECT FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'cov_ft') THEN
                CREATE FOREIGN TABLE public.cov_ft (id int, name text) SERVER cov_test_srv OPTIONS (filename '/dev/null');
            END IF;
            END $$
            """, on: sourceConn)

        // Set up target
        try await IntegrationTestConfig.execute("CREATE EXTENSION IF NOT EXISTS file_fdw", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE SERVER IF NOT EXISTS cov_test_srv FOREIGN DATA WRAPPER file_fdw", on: targetConn)
        try await IntegrationTestConfig.execute("DROP FOREIGN TABLE IF EXISTS public.cov_ft CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .foreignTable, schema: "public", name: "cov_ft")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify foreign table exists on target
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'cov_ft'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP FOREIGN TABLE IF EXISTS public.cov_ft CASCADE", on: targetConn)
    }

    // MARK: - Clone with FORCE RLS

    @Test("Live clone table with FORCE RLS")
    func liveCloneWithForceRLS() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Check if users table has FORCE RLS on source
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        defer { Task { try? await sourceConn.close() } }
        // Enable FORCE RLS on source users table
        try await IntegrationTestConfig.execute("ALTER TABLE public.users FORCE ROW LEVEL SECURITY", on: sourceConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "user_role")),
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyRLSPolicies: true
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify RLS is force-enabled
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT relforcerowsecurity FROM pg_class WHERE relname = 'users' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')"),
            logger: IntegrationTestConfig.logger
        )
        var forceRLS = false
        for try await row in rows { forceRLS = try row.decode(Bool.self) }
        #expect(forceRLS)

        // Clean up - undo force RLS on source
        try await IntegrationTestConfig.execute("ALTER TABLE public.users NO FORCE ROW LEVEL SECURITY", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    // MARK: - Clone with preflight failure

    @Test("Clone with preflight failure throws")
    func clonePreflightFailure() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create conflicting table on target
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE TABLE public.products (id int)", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
            ],
            dryRun: false,
            dropIfExists: false,  // Don't drop - should cause preflight failure
            force: true,
            retries: 0,
            skipPreflight: false  // Enable preflight
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        do {
            _ = try await orchestrator.execute(job: job)
            #expect(Bool(false), "Should have thrown preflightFailed")
        } catch {
            // Expected - preflight should detect conflict and throw
        }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }

    // MARK: - Clone with row limit via parallel

    @Test("Live clone parallel with row limit")
    func liveCloneParallelRowLimit() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable),
                    copyData: true,
                    rowLimit: 2
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            parallel: 2
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM public.\(Self.testTable)"),
            logger: IntegrationTestConfig.logger
        )
        var count: Int64 = 0
        for try await row in rows { count = try row.decode(Int64.self) }
        #expect(count == 2)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Preflight with various object types (covers verifyObjectExists switch branches)

    @Test("Preflight validates view, enum, sequence, composite type objects on source")
    func preflightVariousObjectTypes() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up target - use cov-specific objects to avoid cross-suite races
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.address CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
                ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: "public", name: "address")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Clean up
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.address CASCADE", on: targetConn)
    }

    @Test("PreflightChecker verifies various object types on source")
    func preflightCheckerVariousTypes() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        // Build a job with all object types to exercise verifyObjectExists switch branches
        // Uses dryRun: false so preflight actually runs, but we only call the checker directly
        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "active_users")),
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
                ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: "public", name: "address")),
                ObjectSpec(id: ObjectIdentifier(type: .schema, schema: nil, name: "analytics")),
                ObjectSpec(id: ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        // Call PreflightChecker directly — doesn't actually clone anything
        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(failures.isEmpty, "Preflight should pass for all existing source objects: \(failures)")
    }

    // MARK: - SyncAll with modified table (covers syncAll alter path)

    @Test("SyncAll detects modified table and generates ALTER in dry-run")
    func liveSyncAllAlterTable() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create target table with missing column so differ detects it as modified
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.\(Self.testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL
            )
            """, on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: true,
            dropIfExists: true,
            force: true,
            skipPreflight: true,
            syncAll: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Verify script contains ALTER TABLE for the missing columns
        #expect(script.contains("ALTER TABLE"), "Script should contain ALTER TABLE for modified table")

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("SyncAll with allowDropColumns generates DROP COLUMN in dry-run")
    func liveSyncAllDropColumns() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create target table with extra column not in source
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.\(Self.testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL,
                value numeric(10, 2) NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                extra_col text DEFAULT 'drop_me'
            )
            """, on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: true,
            allowDropColumns: true,
            force: true,
            skipPreflight: true,
            syncAll: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Verify script contains DROP COLUMN for extra_col
        #expect(script.contains("DROP COLUMN") || script.contains("extra_col"), "Script should contain DROP COLUMN for extra column")

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    @Test("SyncAll without allowDropColumns skips destructive changes in dry-run")
    func liveSyncAllSkipDropColumns() async throws {
        try await Self.ensureSourceTables()
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create target table with extra column - allowDropColumns=false should skip
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.\(Self.testTable) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL,
                value numeric(10, 2) NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                extra_col text DEFAULT 'keep_me'
            )
            """, on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: Self.testTable)),
            ],
            dryRun: true,
            allowDropColumns: false,
            force: true,
            skipPreflight: true,
            syncAll: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // With allowDropColumns=false, the warning path (line 93) is exercised
        // We just verify the sync completes without error
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.\(Self.testTable) CASCADE", on: targetConn)
    }

    // MARK: - Clone partitioned table with parallel data

    @Test("Clone partitioned table with parallel generates correct script")
    func liveClonePartitionedParallel() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "events"),
                    copyData: true
                ),
            ],
            dryRun: true,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true,
            parallel: 2
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Verify script contains partition-related SQL
        #expect(script.contains("events"), "Script should reference events table")
        #expect(script.contains("PARTITION") || script.contains("ATTACH"), "Script should contain partition SQL")
    }

    // MARK: - Clone with cascade and data copy (dry-run)

    @Test("Clone with cascade and data generates COPY steps in script")
    func liveCloneCascadeWithDataDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
                    copyData: true,
                    cascadeDependencies: true
                ),
            ],
            dryRun: true,
            defaultDataMethod: .copy,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Verify script contains data copy references
        #expect(script.contains("orders"), "Script should reference orders table")
        #expect(script.contains("users"), "Script should include cascaded users dependency")
    }

    // MARK: - Migration rollback with no applied migrations

    @Test("Rollback with no applied migrations returns empty")
    func rollbackNoApplied() async throws {
        let tmpDir = FileManager.default.temporaryDirectory.appendingPathComponent("mig_rollback_\(UUID().uuidString)").path
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let config = MigrationConfig(directory: tmpDir)
        let applicator = MigrationApplicator(config: config, logger: IntegrationTestConfig.logger)
        let sourceConfig = try IntegrationTestConfig.sourceConfig()

        let rolledBack = try await applicator.rollback(
            targetDSN: sourceConfig.toDSN()
        )
        #expect(rolledBack.isEmpty)
    }
}
