import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Advanced Clone Integration Tests", .tags(.integration), .serialized)
struct AdvancedCloneIntegrationTests {

    private static let testSchema = "_adv_clone_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - RLS Policy Cloning

    @Test("Live clone of table with RLS policies copies RLS settings")
    func liveCloneWithRLSPolicies() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up
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

        // Verify RLS is enabled on target
        let rows = try await targetConn.query(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'users' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let rlsEnabled = try row.decode(Bool.self)
            #expect(rlsEnabled == true)
        }

        // Verify policies exist
        let policyRows = try await targetConn.query(
            "SELECT polname FROM pg_policy WHERE polrelid = (SELECT oid FROM pg_class WHERE relname = 'users' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public'))",
            logger: IntegrationTestConfig.logger
        )
        var policies: [String] = []
        for try await row in policyRows {
            policies.append(try row.decode(String.self))
        }
        #expect(!policies.isEmpty)

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    // MARK: - Partitioned Table Cloning

    @Test("Live clone of partitioned table creates parent and children")
    func liveClonePartitionedTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.events CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "events"),
                    copyData: true
                ),
            ],
            dryRun: false,
            defaultDataMethod: .copy,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify parent table exists and is partitioned
        let partRows = try await targetConn.query(
            "SELECT relkind FROM pg_class WHERE relname = 'events' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
            logger: IntegrationTestConfig.logger
        )
        for try await row in partRows {
            let relkind = try row.decode(String.self)
            #expect(relkind == "p") // 'p' = partitioned table
        }

        // Verify child partitions exist
        let childRows = try await targetConn.query(
            "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = (SELECT oid FROM pg_class WHERE relname = 'events' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) ORDER BY 1",
            logger: IntegrationTestConfig.logger
        )
        var children: [String] = []
        for try await row in childRows {
            children.append(try row.decode(String.self))
        }
        #expect(children.count == 2)

        // Verify data was copied into partitions
        let dataRows = try await targetConn.query(
            "SELECT count(*) FROM public.events",
            logger: IntegrationTestConfig.logger
        )
        for try await row in dataRows {
            let count = try row.decode(Int.self)
            #expect(count > 0)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.events CASCADE", on: targetConn)
    }

    // MARK: - Materialized View Cloning

    @Test("Live clone of materialized view with data refresh")
    func liveCloneMaterializedViewWithData() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // First clone the base tables that the materialized view depends on
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.order_items CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS analytics", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "user_role")),
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "users"), copyData: true),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products"), copyData: true),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "orders"), copyData: true),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "order_items"), copyData: true),
                ObjectSpec(
                    id: ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary"),
                    copyData: true  // This should trigger REFRESH MATERIALIZED VIEW
                ),
            ],
            dryRun: false,
            defaultDataMethod: .copy,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify materialized view exists and has data
        let rows = try await targetConn.query(
            "SELECT count(*) FROM analytics.daily_order_summary",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count > 0)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.order_items CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    // MARK: - Permission Cloning

    @Test("Live clone with permissions copies GRANT statements")
    func liveCloneWithPermissions() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Ensure roles exist on target
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly_role') THEN CREATE ROLE readonly_role; END IF;
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_role') THEN CREATE ROLE app_role; END IF;
            END $$
        """, on: targetConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
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

        // Verify some permissions were applied
        let rows = try await targetConn.query(
            "SELECT grantee, privilege_type FROM information_schema.table_privileges WHERE table_schema = 'public' AND table_name = 'products' AND grantee IN ('readonly_role', 'app_role')",
            logger: IntegrationTestConfig.logger
        )
        var grants: [String] = []
        for try await (grantee, privilege) in rows.decode((String, String).self) {
            grants.append("\(grantee):\(privilege)")
        }
        #expect(!grants.isEmpty)

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }

    // MARK: - Function Cloning

    @Test("Live clone of function creates function on target")
    func liveCloneFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // The function depends on order_items which depends on orders/products/users/enums
        // So let's first create the base tables, then clone just the function
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS public.calculate_order_total(integer) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.order_items CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)

        // Clone all dependencies first, then the function
        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "user_role")),
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "users")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "orders")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "order_items")),
                ObjectSpec(
                    id: ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total", signature: "(integer)")
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

        // Verify function exists on target
        let rows = try await targetConn.query(
            "SELECT proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'calculate_order_total'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)

        // Clean up
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS public.calculate_order_total(integer) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.order_items CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    // MARK: - Schema Cloning

    @Test("Live clone of schema creates schema on target")
    func liveCloneSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP SCHEMA IF EXISTS analytics CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .schema, name: "analytics")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify schema exists
        let rows = try await targetConn.query(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'analytics'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)
    }

    // MARK: - Preflight Checker

    @Test("Preflight detects conflicts on target when drop-existing is false")
    func preflightConflictDetection() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Create a table on target that will conflict
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_test CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).conflict_test (id integer)", on: targetConn)
        try? await targetConn.close()

        // Also create it on source
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await Self.ensureTestSchema(on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).conflict_test (id integer)", on: sourceConn)
        try? await sourceConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_test"))
            ],
            dryRun: false,
            dropIfExists: false,  // Don't drop existing — should detect conflict
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(failures.contains { $0.contains("already exists") })

        // Clean up
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_test CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_test CASCADE", on: tc)
        try? await sc.close()
        try? await tc.close()
    }

    @Test("Preflight validates psql availability for live execution")
    func preflightPsqlCheck() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "users"))
            ],
            dryRun: false,  // Live mode — should check psql
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        // psql should be available in the CI Docker environment
        let psqlFailures = failures.filter { $0.contains("psql") }
        #expect(psqlFailures.isEmpty)
    }

    // MARK: - Dry Run of Partitioned Tables

    @Test("Dry-run clone of partitioned table generates partition SQL")
    func dryRunPartitionedTable() async throws {
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
            defaultDataMethod: .copy,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Should contain PARTITION BY in parent table
        #expect(script.contains("PARTITION BY"))
        // Should contain ATTACH PARTITION for children
        #expect(script.contains("ATTACH PARTITION"))
        // Should contain data copy commands for partitions
        #expect(script.contains("\\copy"))
    }

    // MARK: - Dry Run with RLS

    @Test("Dry-run clone with RLS policies generates RLS SQL")
    func dryRunWithRLS() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

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
            dryRun: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("ENABLE ROW LEVEL SECURITY"))
        #expect(script.contains("CREATE POLICY"))
    }

    // MARK: - Dry Run with Materialized View Refresh

    @Test("Dry-run clone of materialized view with data includes REFRESH")
    func dryRunMaterializedViewRefresh() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary"),
                    copyData: true
                ),
            ],
            dryRun: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("REFRESH MATERIALIZED VIEW"))
    }

    // MARK: - Extension Introspection

    @Test("Describe extension introspection works")
    func describeExtension() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        // Create plpgsql extension if not exists (it's usually there by default)
        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)

        // plpgsql is always available in PostgreSQL
        let id = ObjectIdentifier(type: .extension, name: "plpgsql")
        let metadata = try await introspector.describeExtension(id)
        #expect(metadata.id.name == "plpgsql")
    }

    // MARK: - Cascade Dependency Clone

    @Test("Dry-run clone with cascade resolves all dependencies")
    func dryRunCascadeClone() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
                    cascadeDependencies: true
                ),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Cascade should discover users table (FK dependency)
        #expect(script.contains("\"users\""))
        #expect(script.contains("\"orders\""))
    }
}
