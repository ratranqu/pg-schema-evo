import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Introspection Integration Tests", .tags(.integration))
struct IntrospectionIntegrationTests {

    @Test("Introspect users table columns")
    func introspectUsersColumns() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let metadata = try await introspector.describeTable(id)

        #expect(metadata.columns.count >= 5)

        let columnNames = metadata.columns.map(\.name)
        #expect(columnNames.contains("id"))
        #expect(columnNames.contains("username"))
        #expect(columnNames.contains("email"))
        #expect(columnNames.contains("role"))
        #expect(columnNames.contains("created_at"))
    }

    @Test("Introspect users table constraints")
    func introspectUsersConstraints() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let metadata = try await introspector.describeTable(id)

        let hasPK = metadata.constraints.contains { $0.type == .primaryKey }
        #expect(hasPK)
        let hasUnique = metadata.constraints.contains { $0.type == .unique }
        #expect(hasUnique)
    }

    @Test("Introspect orders table with foreign keys and indexes")
    func introspectOrdersTable() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let metadata = try await introspector.describeTable(id)

        let hasFK = metadata.constraints.contains { $0.type == .foreignKey }
        #expect(hasFK)
        #expect(!metadata.indexes.isEmpty)
    }

    @Test("Introspect order_items table with trigger")
    func introspectOrderItemsTrigger() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "order_items")
        let metadata = try await introspector.describeTable(id)

        #expect(!metadata.triggers.isEmpty)
        let hasTrigger = metadata.triggers.contains { $0.name == "trg_update_order_total" }
        #expect(hasTrigger)
    }

    @Test("Get relation size")
    func relationSize() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let size = try await introspector.relationSize(id)
        #expect(size != nil)
        #expect(size! > 0)
    }

    @Test("List tables")
    func listTables() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let objects = try await introspector.listObjects(schema: "public", types: [.table])
        let names = objects.map(\.name)
        #expect(names.contains("users"))
        #expect(names.contains("products"))
        #expect(names.contains("orders"))
        #expect(names.contains("order_items"))
    }

    @Test("Get table permissions")
    func tablePermissions() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let grants = try await introspector.permissions(for: id)
        #expect(!grants.isEmpty)
    }

    @Test("Object not found throws error")
    func objectNotFound() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "nonexistent_table_xyz")
        await #expect(throws: PGSchemaEvoError.self) {
            try await introspector.describeTable(id)
        }
    }

    // MARK: - New object type introspection tests

    @Test("Introspect view definition")
    func introspectView() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let metadata = try await introspector.describeView(id)

        #expect(metadata.definition.contains("users"))
        #expect(!metadata.columns.isEmpty)
    }

    @Test("Introspect materialized view")
    func introspectMaterializedView() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary")
        let metadata = try await introspector.describeMaterializedView(id)

        #expect(metadata.definition.contains("orders"))
    }

    @Test("Introspect standalone sequence")
    func introspectSequence() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")
        let metadata = try await introspector.describeSequence(id)

        #expect(metadata.startValue == 1000)
        #expect(metadata.increment == 1)
    }

    @Test("Introspect enum type labels")
    func introspectEnum() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .enum, schema: "public", name: "order_status")
        let metadata = try await introspector.describeEnum(id)

        #expect(metadata.labels.contains("pending"))
        #expect(metadata.labels.contains("shipped"))
        #expect(metadata.labels.contains("delivered"))
    }

    @Test("Introspect function")
    func introspectFunction() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .function, schema: "public", name: "calculate_order_total")
        let metadata = try await introspector.describeFunction(id)

        #expect(metadata.language == "sql")
        #expect(metadata.volatility == "STABLE")
        #expect(metadata.returnType != nil)
    }

    @Test("Introspect schema")
    func introspectSchema() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .schema, name: "analytics")
        let metadata = try await introspector.describeSchema(id)

        #expect(!metadata.owner.isEmpty)
    }

    @Test("Introspect role")
    func introspectRole() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .role, name: "readonly_role")
        let metadata = try await introspector.describeRole(id)

        #expect(metadata.canLogin == false)
        #expect(metadata.isSuperuser == false)
    }

    @Test("List multiple object types")
    func listMultipleTypes() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let objects = try await introspector.listObjects(schema: "public", types: [.table, .view, .sequence, .enum])

        let types = Set(objects.map(\.type))
        #expect(types.contains(.table))
        #expect(types.contains(.view))
        #expect(types.contains(.sequence))
        #expect(types.contains(.enum))
    }

    @Test("Resolve dependencies for orders table")
    func resolveDependencies() async throws {
        let config = try IntegrationTestConfig.sourceConfig()
        let connection = try await IntegrationTestConfig.connect(to: config)
        defer { Task { try? await connection.close() } }

        let introspector = PGCatalogIntrospector(
            connection: connection,
            logger: IntegrationTestConfig.logger
        )

        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let deps = try await introspector.dependencies(for: id)

        // orders depends on users (FK) and/or order_status (enum)
        let depNames = deps.map(\.name)
        #expect(depNames.contains("users") || depNames.contains("order_status"))
    }
}

extension Tag {
    @Tag static var integration: Self
}
