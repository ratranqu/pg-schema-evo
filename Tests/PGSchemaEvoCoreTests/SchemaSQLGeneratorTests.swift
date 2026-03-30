import Testing
@testable import PGSchemaEvoCore

@Suite("SchemaSQLGenerator Tests")
struct SchemaSQLGeneratorTests {
    let gen = SchemaSQLGenerator()

    @Test("Generate CREATE SCHEMA")
    func createSchema() throws {
        let id = ObjectIdentifier(type: .schema, name: "analytics")
        let metadata = SchemaMetadata(id: id, owner: "postgres")
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE SCHEMA IF NOT EXISTS"))
        #expect(sql.contains("\"analytics\""))
        #expect(sql.contains("ALTER SCHEMA"))
        #expect(sql.contains("OWNER TO"))
    }

    @Test("Generate CREATE ROLE with options")
    func createRole() throws {
        let id = ObjectIdentifier(type: .role, name: "readonly_role")
        let metadata = RoleMetadata(
            id: id,
            canLogin: false,
            isSuperuser: false,
            canCreateDB: false,
            canCreateRole: false,
            connectionLimit: -1,
            memberOf: []
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE ROLE"))
        #expect(sql.contains("\"readonly_role\""))
        #expect(sql.contains("NOLOGIN"))
        #expect(sql.contains("IF NOT EXISTS"))
    }

    @Test("Generate CREATE ROLE with membership")
    func createRoleWithMembership() throws {
        let id = ObjectIdentifier(type: .role, name: "app_user")
        let metadata = RoleMetadata(
            id: id,
            canLogin: true,
            memberOf: ["app_role", "readonly_role"]
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("LOGIN"))
        #expect(sql.contains("GRANT \"app_role\" TO \"app_user\""))
        #expect(sql.contains("GRANT \"readonly_role\" TO \"app_user\""))
    }

    @Test("Generate CREATE EXTENSION")
    func createExtension() throws {
        let id = ObjectIdentifier(type: .extension, name: "uuid-ossp")
        let metadata = ExtensionMetadata(id: id, version: "1.1", installedSchema: "public")
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE EXTENSION IF NOT EXISTS"))
        #expect(sql.contains("\"uuid-ossp\""))
        #expect(sql.contains("VERSION '1.1'"))
    }

    @Test("Generate DROP for each type")
    func dropTypes() {
        let schemaId = ObjectIdentifier(type: .schema, name: "test")
        #expect(gen.generateDrop(for: schemaId).contains("DROP SCHEMA IF EXISTS"))

        let roleId = ObjectIdentifier(type: .role, name: "test")
        #expect(gen.generateDrop(for: roleId).contains("DROP ROLE IF EXISTS"))

        let extId = ObjectIdentifier(type: .extension, name: "test")
        #expect(gen.generateDrop(for: extId).contains("DROP EXTENSION IF EXISTS"))
    }
}
