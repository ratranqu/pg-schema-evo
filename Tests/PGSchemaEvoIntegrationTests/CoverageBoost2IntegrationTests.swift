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

    // MARK: - Introspector: describeTable

    @Test("Describe table via introspector returns columns")
    func describeTable() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let metadata = try await introspector.describeTable(tableId)

        #expect(metadata.columns.contains { $0.name == "id" })
        #expect(metadata.columns.contains { $0.name == "username" })
        #expect(metadata.columns.contains { $0.name == "email" })
    }

    // MARK: - Introspector: describeView

    @Test("Describe view via introspector returns definition")
    func describeView() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let viewId = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let metadata = try await introspector.describeView(viewId)

        #expect(metadata.definition.contains("users"))
    }

    // MARK: - Introspector: describeSequence

    @Test("Describe sequence via introspector")
    func describeSequence() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")
        let metadata = try await introspector.describeSequence(seqId)

        #expect(metadata.startValue == 1000)
        #expect(metadata.increment == 1)
    }

    // MARK: - Introspector: describeEnum

    @Test("Describe enum via introspector returns labels")
    func describeEnum() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "order_status")
        let metadata = try await introspector.describeEnum(enumId)

        #expect(metadata.labels.contains("pending"))
        #expect(metadata.labels.contains("shipped"))
        #expect(metadata.labels.contains("delivered"))
    }

    // MARK: - Introspector: describeCompositeType

    @Test("Describe composite type via introspector")
    func describeCompositeType() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let typeId = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let metadata = try await introspector.describeCompositeType(typeId)

        #expect(metadata.attributes.contains { $0.name == "street" })
        #expect(metadata.attributes.contains { $0.name == "city" })
    }

    // MARK: - Introspector: describeFunction

    @Test("Describe function via introspector returns definition")
    func describeFunction() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let funcId = ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total")
        let metadata = try await introspector.describeFunction(funcId)

        #expect(metadata.definition.contains("calculate_order_total"))
        #expect(metadata.language == "sql")
    }

    // MARK: - Introspector: permissions

    @Test("List permissions for table")
    func listPermissions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let perms = try await introspector.permissions(for: tableId)

        // The seed grants SELECT to readonly_role and multiple privs to app_role
        #expect(!perms.isEmpty)
    }

    // MARK: - Introspector: RLS policies

    @Test("List RLS policies for users table")
    func listRLSPolicies() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let rlsInfo = try await introspector.rlsPolicies(for: tableId)

        #expect(rlsInfo.isEnabled == true)
        #expect(rlsInfo.policies.contains { $0.name == "users_self_access" })
    }

    // MARK: - Introspector: partitionInfo

    @Test("Get partition info for events table")
    func partitionInfo() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let partInfo = try await introspector.partitionInfo(for: tableId)

        #expect(partInfo != nil)
        #expect(partInfo?.strategy == "RANGE")
    }

    // MARK: - Introspector: listPartitions

    @Test("List partitions for events table")
    func listPartitions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let partitions = try await introspector.listPartitions(for: tableId)

        #expect(partitions.count >= 2)
        #expect(partitions.contains { $0.id.name == "events_2025q1" })
        #expect(partitions.contains { $0.id.name == "events_2025q2" })
    }

    // MARK: - Introspector: primaryKeyColumns

    @Test("Get primary key columns for users table")
    func primaryKeyColumns() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let pkCols = try await introspector.primaryKeyColumns(for: tableId)

        #expect(pkCols == ["id"])
    }

    // MARK: - Introspector: relationSize

    @Test("Get relation size for users table")
    func relationSize() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let size = try await introspector.relationSize(tableId)

        #expect(size != nil)
        #expect(size! > 0)
    }

    // MARK: - Introspector: dependencies

    @Test("Get dependencies for orders table")
    func dependencies() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let deps = try await introspector.dependencies(for: tableId)

        // orders depends on users (FK) and order_status (enum)
        #expect(!deps.isEmpty)
    }

    // MARK: - Introspector: describeRole

    @Test("Describe role via introspector")
    func describeRole() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let roleId = ObjectIdentifier(type: .role, name: "readonly_role")
        let metadata = try await introspector.describeRole(roleId)

        #expect(metadata.id.name == "readonly_role")
    }

    // MARK: - Introspector: describeSchema

    @Test("Describe schema via introspector")
    func describeSchema() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let schemaId = ObjectIdentifier(type: .schema, name: "analytics")
        let metadata = try await introspector.describeSchema(schemaId)

        #expect(metadata.id.name == "analytics")
    }

    // MARK: - Introspector: describeMaterializedView

    @Test("Describe materialized view via introspector")
    func describeMaterializedView() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let conn = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await conn.close() } }

        let introspector = PGCatalogIntrospector(connection: conn, logger: IntegrationTestConfig.logger)
        let mvId = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary")
        let metadata = try await introspector.describeMaterializedView(mvId)

        #expect(metadata.definition.contains("order_date") || metadata.definition.contains("orders"))
    }
}
