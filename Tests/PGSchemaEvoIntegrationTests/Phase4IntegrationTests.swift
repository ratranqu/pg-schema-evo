import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Phase 4 Integration Tests", .tags(.integration))
struct Phase4IntegrationTests {

    @Test("Schema diff detects objects only in source")
    func schemaDiffOnlyInSource() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let result = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: "public",
            types: [.table]
        )

        // Source has tables that target shouldn't have
        #expect(!result.onlyInSource.isEmpty)
        let names = result.onlyInSource.map(\.name)
        #expect(names.contains("users"))
    }

    @Test("Schema diff renders text output")
    func schemaDiffTextOutput() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let result = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: "public"
        )

        let text = result.renderText()
        #expect(text.contains("Summary:"))
    }

    @Test("Pre-flight checker validates source connectivity")
    func preflightSourceConnectivity() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "users"))
            ],
            dryRun: true
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        // Source should be connectable, but object may exist on target
        let connectionFailures = failures.filter { $0.contains("Cannot connect") }
        #expect(connectionFailures.isEmpty)
    }

    @Test("Pre-flight checker detects missing objects")
    func preflightMissingObject() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "nonexistent_table_xyz"))
            ],
            dryRun: true
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)

        #expect(failures.contains { $0.contains("nonexistent_table_xyz") })
    }

    @Test("RLS policies are introspected")
    func rlsPoliciesIntrospection() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(connection: connection, logger: IntegrationTestConfig.logger)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let rlsInfo = try await introspector.rlsPolicies(for: id)

        #expect(rlsInfo.isEnabled == true)
        #expect(!rlsInfo.policies.isEmpty)
        let policyNames = rlsInfo.policies.map(\.name)
        #expect(policyNames.contains("users_self_access"))
    }

    @Test("Partition info is introspected")
    func partitionInfoIntrospection() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(connection: connection, logger: IntegrationTestConfig.logger)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let partInfo = try await introspector.partitionInfo(for: id)

        #expect(partInfo != nil)
        #expect(partInfo?.strategy == "RANGE")
    }

    @Test("Partition children are listed")
    func partitionChildrenListed() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(connection: connection, logger: IntegrationTestConfig.logger)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let children = try await introspector.listPartitions(for: id)

        #expect(children.count == 2)
        let childNames = children.map(\.id.name)
        #expect(childNames.contains("events_2025q1"))
        #expect(childNames.contains("events_2025q2"))
    }

    @Test("Live clone with WHERE filter copies subset of data")
    func liveCloneWithWhereFilter() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyData: true,
                    whereClause: "price > 20"
                ),
            ],
            dryRun: false,
            force: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify filtered data was copied (only products with price > 20)
        let rows = try await targetConn.query(
            "SELECT count(*) FROM public.products",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            // Source has 4 products, only 2 have price > 20 (Widget B=24.99, Gadget X=49.99)
            #expect(count == 2)
        }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }

    @Test("Live clone with row limit restricts data")
    func liveCloneWithRowLimit() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)

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
            dryRun: false,
            force: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        let rows = try await targetConn.query(
            "SELECT count(*) FROM public.products",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 2)
        }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.products CASCADE", on: targetConn)
    }
}
