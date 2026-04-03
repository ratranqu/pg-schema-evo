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

    // MARK: - Dry Run: RLS Policy Path

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

    // MARK: - Dry Run: Partitioned Table Path

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

    // MARK: - Dry Run: Materialized View Refresh Path

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

    // MARK: - Dry Run: Permission Path

    @Test("Dry-run clone with permissions includes GRANT statements")
    func dryRunWithPermissions() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyPermissions: true
                ),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("GRANT"))
    }

    // MARK: - Dry Run: Function Path

    @Test("Dry-run clone of function generates CREATE FUNCTION SQL")
    func dryRunFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total", signature: "(integer)")
                ),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE") || script.contains("FUNCTION"))
        #expect(script.contains("calculate_order_total"))
    }

    // MARK: - Dry Run: Cascade Dependency Resolution

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

    // MARK: - Dry Run: Full materialized view with dependencies

    @Test("Dry-run clone of materialized view and base tables generates complete script")
    func dryRunMaterializedViewComplete() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

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
                    copyData: true
                ),
            ],
            dryRun: true,
            defaultDataMethod: .copy,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("REFRESH MATERIALIZED VIEW"))
        #expect(script.contains("\\copy"))
        #expect(script.contains("\"users\""))
        #expect(script.contains("\"order_items\""))
    }

    // MARK: - Live Clone: Schema (safe — unique name, no conflicts)

    @Test("Live clone of schema creates schema on target")
    func liveCloneSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        // Create a unique schema on source to clone
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(Self.testSchema)", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SCHEMA IF EXISTS \(Self.testSchema) CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .schema, name: Self.testSchema)),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify schema exists on target
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT 1 FROM information_schema.schemata WHERE schema_name = '\(Self.testSchema)'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)
    }

    // MARK: - Live Clone: Isolated table with permissions

    @Test("Live clone with permissions in isolated schema")
    func liveCloneWithPermissions() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Setup isolated test data on source
        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).perm_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).perm_test CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).perm_test (id integer PRIMARY KEY, name text)
        """, on: sourceConn)

        // Create roles and grant permissions on source
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly_role') THEN CREATE ROLE readonly_role; END IF;
            END $$
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("GRANT SELECT ON \(Self.testSchema).perm_test TO readonly_role", on: sourceConn)

        // Ensure role exists on target too
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly_role') THEN CREATE ROLE readonly_role; END IF;
            END $$
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: Self.testSchema, name: "perm_test"),
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

        // Verify table exists and permissions were applied
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT grantee, privilege_type FROM information_schema.table_privileges WHERE table_schema = '\(Self.testSchema)' AND table_name = 'perm_test' AND grantee = 'readonly_role'"),
            logger: IntegrationTestConfig.logger
        )
        var grants: [String] = []
        for try await (grantee, privilege) in rows.decode((String, String).self) {
            grants.append("\(grantee):\(privilege)")
        }
        #expect(!grants.isEmpty)

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).perm_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).perm_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Preflight Checker (uses isolated schema — safe)

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
            dropIfExists: false,
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
            dryRun: false,
            force: true,
            retries: 0,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        let psqlFailures = failures.filter { $0.contains("psql") }
        #expect(psqlFailures.isEmpty)
    }

    // MARK: - Extension Introspection (read-only — safe)

    @Test("Describe extension introspection works")
    func describeExtension() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)

        let id = ObjectIdentifier(type: .extension, name: "plpgsql")
        let metadata = try await introspector.describeExtension(id)
        #expect(metadata.id.name == "plpgsql")
    }

    // MARK: - Dry Run: Function with drop

    @Test("Dry-run clone of function with drop generates DROP FUNCTION SQL")
    func dryRunFunctionWithDrop() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .function, schema: "public", name: "update_order_total", signature: "()")
                ),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("DROP FUNCTION IF EXISTS"))
        #expect(script.contains("update_order_total"))
    }

    // MARK: - Dry Run: Sequence with data method

    @Test("Dry-run clone of sequence generates CREATE SEQUENCE SQL")
    func dryRunSequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE SEQUENCE"))
        #expect(script.contains("invoice_number_seq"))
        #expect(script.contains("DROP SEQUENCE IF EXISTS"))
    }

    // MARK: - Dry Run: Enum type

    @Test("Dry-run clone of enum generates CREATE TYPE SQL")
    func dryRunEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TYPE"))
        #expect(script.contains("order_status"))
        #expect(script.contains("DROP TYPE IF EXISTS"))
    }

    // MARK: - Dry Run: Composite type

    @Test("Dry-run clone of composite type generates CREATE TYPE SQL")
    func dryRunCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: "public", name: "address")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TYPE"))
        #expect(script.contains("address"))
    }

    // MARK: - Dry Run: Schema

    @Test("Dry-run clone of schema generates CREATE SCHEMA SQL")
    func dryRunSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .schema, name: "analytics")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE SCHEMA"))
        #expect(script.contains("analytics"))
    }
}
