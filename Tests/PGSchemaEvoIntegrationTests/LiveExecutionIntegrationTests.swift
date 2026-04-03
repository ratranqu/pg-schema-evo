import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Live Execution Integration Tests", .tags(.integration), .serialized)
struct LiveExecutionIntegrationTests {

    @Test("Live clone of single table creates table on target")
    func liveCloneSingleTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up first
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products")
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

        // Verify table exists on target
        let rows = try await targetConn.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'products' ORDER BY ordinal_position",
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(columns.contains("id"))
        #expect(columns.contains("name"))
        #expect(columns.contains("price"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }

    @Test("Live clone with data copies rows")
    func liveCloneWithData() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up first
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
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

        // Verify data was copied
        let rows = try await targetConn.query(
            "SELECT count(*) FROM public.products",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count > 0)
        }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }

    @Test("Live clone with drop-existing is idempotent")
    func liveCloneDropExistingIdempotent() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products")
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)

        // Run twice — second run should succeed due to DROP IF EXISTS
        _ = try await orchestrator.execute(job: job)
        _ = try await orchestrator.execute(job: job)

        // Verify table exists
        let rows = try await targetConn.query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'products'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }

    @Test("Live clone of enum type")
    func liveCloneEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .enum, schema: "public", name: "order_status")
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

        // Verify enum exists on target
        let rows = try await targetConn.query(
            "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = 'public' AND t.typname = 'order_status' ORDER BY e.enumsortorder",
            logger: IntegrationTestConfig.logger
        )
        var labels: [String] = []
        for try await row in rows {
            labels.append(try row.decode(String.self))
        }
        #expect(labels.contains("pending"))
        #expect(labels.contains("shipped"))

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
    }

    @Test("Live clone of view")
    func liveCloneView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // View depends on users table and user_role enum
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

        // Verify view exists
        let rows = try await targetConn.query(
            "SELECT 1 FROM information_schema.views WHERE table_schema = 'public' AND table_name = 'active_users'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)

        try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS public.active_users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }

    @Test("Live clone of sequence")
    func liveCloneSequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")
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

        // Verify sequence exists
        let rows = try await targetConn.query(
            "SELECT start_value FROM information_schema.sequences WHERE sequence_schema = 'public' AND sequence_name = 'invoice_number_seq'",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let startValue = try row.decode(String.self)
            #expect(startValue == "1000")
        }

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)
    }

    @Test("Live clone of composite type")
    func liveCloneCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.address CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
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

        // Verify composite type exists
        let rows = try await targetConn.query(
            "SELECT a.attname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace JOIN pg_attribute a ON a.attrelid = t.typrelid WHERE n.nspname = 'public' AND t.typname = 'address' AND t.typtype = 'c' AND a.attnum > 0 ORDER BY a.attnum",
            logger: IntegrationTestConfig.logger
        )
        var attrs: [String] = []
        for try await row in rows {
            attrs.append(try row.decode(String.self))
        }
        #expect(attrs.contains("street"))
        #expect(attrs.contains("city"))
        #expect(attrs.contains("country"))

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.address CASCADE", on: targetConn)
    }

    @Test("Live clone multiple tables with FK dependencies in correct order")
    func liveCloneWithFKDependencies() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clean up in reverse dependency order
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)

        // Request orders first — cascade should discover users and enums
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

        // Verify both tables exist (orders depends on users via FK)
        let rows = try await targetConn.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('users', 'orders') ORDER BY table_name",
            logger: IntegrationTestConfig.logger
        )
        var tables: [String] = []
        for try await row in rows {
            tables.append(try row.decode(String.self))
        }
        #expect(tables.contains("orders"))
        #expect(tables.contains("users"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.orders CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.users CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.user_role CASCADE", on: targetConn)
    }
}
