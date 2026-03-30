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

        let constraintNames = metadata.constraints.map(\.name)
        // Should have primary key
        let hasPK = metadata.constraints.contains { $0.type == .primaryKey }
        #expect(hasPK)
        // Should have unique constraint on username
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

        // Should have FK to users
        let hasFK = metadata.constraints.contains { $0.type == .foreignKey }
        #expect(hasFK)

        // Should have indexes
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

        // Should have at least the readonly_role and app_role grants
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
}

extension Tag {
    @Tag static var integration: Self
}
