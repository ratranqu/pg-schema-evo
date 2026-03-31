import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Sync Integration Tests", .tags(.integration), .serialized)
struct SyncIntegrationTests {

    // Use sequence (no dependencies) to avoid table conflicts with other test suites
    private static let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")

    @Test("Sync detects no changes when target matches source")
    func syncNoChanges() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Clone the sequence to target first
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)

        let cloneJob = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: Self.seqId)],
            dryRun: false,
            force: true,
            retries: 0,
            skipPreflight: true
        )
        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: cloneJob)

        // Now sync — should find no changes
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: Self.seqId)],
            dryRun: false,
            force: true
        )
        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await syncOrchestrator.execute(job: syncJob)

        #expect(output.contains("already in sync"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)
    }

    @Test("Sync creates missing sequence on target")
    func syncCreatesMissingSequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Ensure sequence doesn't exist on target
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)

        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: Self.seqId)],
            dryRun: false,
            force: true
        )
        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify sequence was created
        let rows = try await targetConn.query(
            "SELECT relname FROM pg_class WHERE relname = 'invoice_number_seq' AND relkind = 'S'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)

        // Clean up
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)
    }

    @Test("Sync dry-run returns script without executing")
    func syncDryRun() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Ensure sequence doesn't exist on target
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)

        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: Self.seqId)],
            dryRun: true
        )
        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await syncOrchestrator.execute(job: syncJob)

        // Should return a script
        #expect(output.contains("#!/usr/bin/env bash"))
        #expect(output.contains("CREATE SEQUENCE"))

        // Sequence should NOT have been created
        let rows = try await targetConn.query(
            "SELECT count(*) FROM pg_class WHERE relname = 'invoice_number_seq' AND relkind = 'S'",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 0)
        }
    }

    @Test("Sync enum type creates missing enum")
    func syncEnumType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "order_status")

        // Ensure enum doesn't exist on target
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)

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
        let rows = try await targetConn.query(
            "SELECT typname FROM pg_type WHERE typname = 'order_status' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found)

        // Clean up
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS public.order_status CASCADE", on: targetConn)
    }

    @Test("Sync with drop-extra removes target-only sequences")
    func syncDropExtra() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create an extra sequence on target that doesn't exist on source
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.sync_test_extra_seq CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute(
            "CREATE SEQUENCE public.sync_test_extra_seq",
            on: targetConn
        )

        let extraSeqId = ObjectIdentifier(type: .sequence, schema: "public", name: "sync_test_extra_seq")

        // Sync with drop-extra
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: extraSeqId)],
            dryRun: false,
            dropExtra: true,
            force: true
        )
        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify extra sequence was dropped
        let rows = try await targetConn.query(
            "SELECT count(*) FROM pg_class WHERE relname = 'sync_test_extra_seq' AND relkind = 'S'",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 0)
        }
    }
}
