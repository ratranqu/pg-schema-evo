import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

/// Integration tests that exercise CloneOrchestrator, SyncOrchestrator,
/// PreflightChecker, and related execution paths.
/// These tests use dry-run mode to avoid destructive changes where possible.
@Suite("Orchestrator Integration Tests")
struct OrchestratorIntegrationTests {

    // MARK: - CloneOrchestrator dry-run tests

    @Test("Clone orchestrator dry-run produces script for table")
    func cloneDryRunTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("products"))
    }

    @Test("Clone orchestrator dry-run with data produces COPY")
    func cloneDryRunWithData() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyData: true
                ),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("COPY"))
    }

    @Test("Clone orchestrator dry-run with enum")
    func cloneDryRunEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TYPE"))
        #expect(script.contains("order_status"))
        #expect(script.contains("pending"))
    }

    @Test("Clone orchestrator dry-run with view")
    func cloneDryRunView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .view, schema: "public", name: "active_users")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE"))
        #expect(script.contains("VIEW"))
        #expect(script.contains("active_users"))
    }

    @Test("Clone orchestrator dry-run with sequence")
    func cloneDryRunSequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE SEQUENCE"))
        #expect(script.contains("invoice_number_seq"))
    }

    @Test("Clone orchestrator dry-run with function")
    func cloneDryRunFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("FUNCTION"))
        #expect(script.contains("calculate_order_total"))
    }

    @Test("Clone orchestrator dry-run with composite type")
    func cloneDryRunCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: "public", name: "address")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TYPE"))
        #expect(script.contains("address"))
    }

    @Test("Clone orchestrator dry-run with materialized view")
    func cloneDryRunMaterializedView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("MATERIALIZED VIEW"))
        #expect(script.contains("daily_order_summary"))
    }

    @Test("Clone orchestrator dry-run with permissions")
    func cloneDryRunWithPermissions() async throws {
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
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("products"))
    }

    @Test("Clone orchestrator dry-run with dropIfExists")
    func cloneDryRunDropIfExists() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
            ],
            dryRun: true,
            dropIfExists: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("DROP"))
        #expect(script.contains("CREATE TABLE"))
    }

    @Test("Clone orchestrator dry-run with RLS policies")
    func cloneDryRunWithRLS() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyRLSPolicies: true
                ),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("users"))
    }

    @Test("Clone orchestrator dry-run multiple objects")
    func cloneDryRunMultipleObjects() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("order_status"))
        #expect(script.contains("products"))
        #expect(script.contains("invoice_number_seq"))
    }

    @Test("Clone orchestrator dry-run with row limit")
    func cloneDryRunWithRowLimit() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyData: true,
                    rowLimit: 2
                ),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("COPY"))
    }

    @Test("Clone orchestrator dry-run with global row limit")
    func cloneDryRunWithGlobalRowLimit() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyData: true
                ),
            ],
            dryRun: true,
            skipPreflight: true,
            globalRowLimit: 1
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("COPY"))
    }

    // MARK: - SyncOrchestrator dry-run tests

    @Test("Sync orchestrator dry-run with targeted objects")
    func syncDryRunTargeted() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
            ],
            dryRun: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await orchestrator.execute(job: job)

        // Should contain some output about the sync
        #expect(!result.isEmpty)
    }

    @Test("Sync orchestrator dry-run with enum")
    func syncDryRunEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")),
            ],
            dryRun: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await orchestrator.execute(job: job)

        #expect(!result.isEmpty)
    }

    @Test("Sync orchestrator dry-run with syncAll")
    func syncDryRunSyncAll() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "products")),
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

    // MARK: - PostgresConnectionPool tests

    @Test("Connection pool creates and closes connections")
    func connectionPoolCreateAndClose() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let pool = try await PostgresConnectionPool.create(
            config: config,
            size: 2,
            logger: IntegrationTestConfig.logger
        )

        #expect(pool.size == 2)

        // Use a connection
        let result: Int = try await pool.withConnection { conn in
            let rows = try await conn.query("SELECT 1 as val", logger: IntegrationTestConfig.logger)
            var val = 0
            for try await row in rows {
                val = try row.decode(Int.self)
            }
            return val
        }
        #expect(result == 1)

        await pool.close()
    }

    @Test("Connection pool supports concurrent access")
    func connectionPoolConcurrent() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let pool = try await PostgresConnectionPool.create(
            config: config,
            size: 3,
            logger: IntegrationTestConfig.logger
        )

        // Run 5 queries concurrently (pool size 3, so some will queue)
        try await withThrowingTaskGroup(of: Int.self) { group in
            for _ in 0..<5 {
                group.addTask {
                    try await pool.withConnection { conn in
                        let rows = try await conn.query(
                            "SELECT 1 as val",
                            logger: IntegrationTestConfig.logger
                        )
                        var val = 0
                        for try await row in rows {
                            val = try row.decode(Int.self)
                        }
                        return val
                    }
                }
            }

            var results: [Int] = []
            for try await result in group {
                results.append(result)
            }
            #expect(results.count == 5)
        }

        await pool.close()
    }

    @Test("Connection pool withConnection returns connection on error")
    func connectionPoolErrorHandling() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let pool = try await PostgresConnectionPool.create(
            config: config,
            size: 1,
            logger: IntegrationTestConfig.logger
        )

        // Execute a query that fails
        do {
            _ = try await pool.withConnection { conn in
                try await IntegrationTestConfig.execute("SELECT * FROM nonexistent_table_xyz", on: conn)
            }
        } catch {
            // Expected — connection should be returned to pool
        }

        // Connection should still be usable
        let result: Int = try await pool.withConnection { conn in
            let rows = try await conn.query("SELECT 42 as val", logger: IntegrationTestConfig.logger)
            var val = 0
            for try await row in rows {
                val = try row.decode(Int.self)
            }
            return val
        }
        #expect(result == 42)

        await pool.close()
    }

    // MARK: - PostgresConnectionHelper tests

    @Test("PostgresConnectionHelper connects successfully")
    func connectionHelperConnect() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await PostgresConnectionHelper.connect(config: config, logger: IntegrationTestConfig.logger)
        let rows = try await conn.query("SELECT 1 as v", logger: IntegrationTestConfig.logger)
        var count = 0
        for try await _ in rows {
            count += 1
        }
        #expect(count == 1)
        try? await conn.close()
    }

    // MARK: - Clone orchestrator dry-run with schema

    @Test("Clone orchestrator dry-run with schema")
    func cloneDryRunSchema() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .schema, name: "analytics")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("SCHEMA"))
        #expect(script.contains("analytics"))
    }

    @Test("Clone orchestrator dry-run with role")
    func cloneDryRunRole() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .role, name: "readonly_role")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("ROLE"))
        #expect(script.contains("readonly_role"))
    }

    @Test("Clone orchestrator dry-run with extension")
    func cloneDryRunExtension() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .extension, name: "pg_trgm")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("EXTENSION"))
        #expect(script.contains("pg_trgm"))
    }

    @Test("Clone orchestrator dry-run with procedure")
    func cloneDryRunProcedure() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .procedure, schema: "public", name: "reset_order_totals")),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("reset_order_totals"))
    }

    // MARK: - Clone with data and WHERE clause

    @Test("Clone orchestrator dry-run with where clause")
    func cloneDryRunWithWhere() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyData: true,
                    whereClause: "role = 'admin'"
                ),
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("COPY"))
    }
}
