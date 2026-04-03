import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Coverage Boost 2 Integration Tests", .tags(.integration), .serialized)
struct CoverageBoost2IntegrationTests {

    // MARK: - Introspector: listObjects for procedures

    @Test("List procedures via introspector")
    func listProcedures() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: "public", types: [.procedure])

        #expect(objects.contains { $0.name == "reset_order_totals" && $0.type == .procedure })
    }

    // MARK: - Introspector: listObjects for extensions

    @Test("List extensions via introspector")
    func listExtensions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: [.extension])

        #expect(objects.contains { $0.name == "plpgsql" && $0.type == .extension })
    }

    // MARK: - Introspector: listObjects for roles

    @Test("List roles via introspector")
    func listRoles() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: [.role])

        #expect(objects.contains { $0.name == "readonly_role" && $0.type == .role })
    }

    // MARK: - Introspector: listObjects for schemas

    @Test("List schemas via introspector")
    func listSchemas() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: nil, types: [.schema])

        #expect(objects.contains { $0.name == "public" && $0.type == .schema })
        #expect(objects.contains { $0.name == "analytics" && $0.type == .schema })
    }

    // MARK: - Introspector: listObjects for functions

    @Test("List functions via introspector")
    func listFunctions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(schema: "public", types: [.function])

        #expect(objects.contains { $0.name == "calculate_order_total" && $0.type == .function })
    }

    // MARK: - Introspector: describeExtension

    @Test("Describe extension via introspector")
    func describeExtension() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let extId = ObjectIdentifier(type: .extension, name: "pg_trgm")
        let metadata = try await introspector.describeExtension(extId)

        #expect(!metadata.version.isEmpty)
        #expect(metadata.id.name == "pg_trgm")
    }

    // MARK: - Introspector: describe procedure (via describeFunction)

    @Test("Describe procedure via introspector")
    func describeProcedure() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let procId = ObjectIdentifier(type: .procedure, schema: "public", name: "reset_order_totals")
        let metadata = try await introspector.describeFunction(procId)

        #expect(metadata.definition.contains("reset_order_totals"))
        #expect(metadata.language == "plpgsql")
    }

    // MARK: - Introspector: listObjects with multiple types

    @Test("List multiple object types at once")
    func listMultipleTypes() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let objects = try await introspector.listObjects(
            schema: "public",
            types: [.table, .view, .function, .sequence, .enum, .compositeType, .procedure]
        )

        let types = Set(objects.map(\.type))
        #expect(types.contains(.table))
        #expect(types.contains(.view))
        #expect(types.contains(.function))
    }

    // MARK: - SchemaDiffer: diff with same source (identical)

    @Test("SchemaDiffer diff detects identical schemas on same database")
    func differDetectsIdentical() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn1 = try await IntegrationTestConfig.connect(to: config)
        let conn2 = try await IntegrationTestConfig.connect(to: config)

        let source = PGCatalogIntrospector(connection: conn1, logger: IntegrationTestConfig.logger)
        let target = PGCatalogIntrospector(connection: conn2, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let result = try await differ.diff(
            source: source,
            target: target,
            schema: "public",
            types: [.table]
        )

        // Same database — should be identical
        #expect(result.isEmpty)

        try? await conn1.close()
        try? await conn2.close()
    }

    // MARK: - SyncOrchestrator: object not found case

    @Test("Sync handles object not found in source or target gracefully")
    func syncObjectNotFound() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "nonexistent_table_xyz_99"))
            ],
            dryRun: true,
            force: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob)

        // Object not found in either — should return "already in sync"
        #expect(output.contains("already in sync"))
    }

    // MARK: - DataSync: initialize with multiple tables

    @Test("Data sync initialize with multiple tables")
    func dataSyncInitMultiple() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let stateFile = NSTemporaryDirectory() + "data-sync-multi-\(UUID().uuidString).yaml"

        let job = DataSyncJob(
            source: sourceConfig,
            target: sourceConfig,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    trackingColumn: "created_at"
                ),
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
                    trackingColumn: "created_at"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.initialize(job: job)

        #expect(output.contains("public.products"))
        #expect(output.contains("public.orders"))

        let store = DataSyncStateStore()
        let state = try store.load(path: stateFile)
        #expect(state.tables.count == 2)
    }
}
