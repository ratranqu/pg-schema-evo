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

    @Test("Generate CREATE ROLE with all options enabled")
    func createRoleAllOptions() throws {
        let id = ObjectIdentifier(type: .role, name: "super_admin")
        let metadata = RoleMetadata(
            id: id,
            canLogin: true,
            isSuperuser: true,
            canCreateDB: true,
            canCreateRole: true,
            connectionLimit: 10,
            memberOf: []
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("LOGIN"))
        #expect(sql.contains("SUPERUSER"))
        #expect(sql.contains("CREATEDB"))
        #expect(sql.contains("CREATEROLE"))
        #expect(sql.contains("CONNECTION LIMIT 10"))
        #expect(!sql.contains("NOLOGIN"))
    }

    @Test("Generate CREATE ROLE with negative connectionLimit omits CONNECTION LIMIT")
    func createRoleNegativeConnectionLimit() throws {
        let id = ObjectIdentifier(type: .role, name: "basic_role")
        let metadata = RoleMetadata(
            id: id,
            canLogin: false,
            connectionLimit: -1
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("NOLOGIN"))
        #expect(!sql.contains("CONNECTION LIMIT"))
    }

    @Test("Generate CREATE ROLE with memberOf grants")
    func createRoleWithMultipleMemberOf() throws {
        let id = ObjectIdentifier(type: .role, name: "dev_user")
        let metadata = RoleMetadata(
            id: id,
            canLogin: true,
            memberOf: ["developers", "readers", "monitoring"]
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("GRANT \"developers\" TO \"dev_user\""))
        #expect(sql.contains("GRANT \"readers\" TO \"dev_user\""))
        #expect(sql.contains("GRANT \"monitoring\" TO \"dev_user\""))
    }

    @Test("Generate CREATE EXTENSION with non-public schema")
    func createExtensionNonPublicSchema() throws {
        let id = ObjectIdentifier(type: .extension, name: "postgis")
        let metadata = ExtensionMetadata(id: id, version: "3.4.0", installedSchema: "geo")
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("SCHEMA \"geo\""))
        #expect(sql.contains("VERSION '3.4.0'"))
    }

    @Test("Generate CREATE EXTENSION with public schema omits SCHEMA clause")
    func createExtensionPublicSchema() throws {
        let id = ObjectIdentifier(type: .extension, name: "hstore")
        let metadata = ExtensionMetadata(id: id, version: "1.8", installedSchema: "public")
        let sql = try gen.generateCreate(from: metadata)
        #expect(!sql.contains("SCHEMA"))
        #expect(sql.contains("VERSION '1.8'"))
    }

    @Test("Generate CREATE EXTENSION with nil schema omits SCHEMA clause")
    func createExtensionNilSchema() throws {
        let id = ObjectIdentifier(type: .extension, name: "pg_trgm")
        let metadata = ExtensionMetadata(id: id, version: "1.6")
        let sql = try gen.generateCreate(from: metadata)
        #expect(!sql.contains("SCHEMA"))
        #expect(sql.contains("\"pg_trgm\""))
    }

    @Test("Generate DROP for unsupported type returns comment")
    func dropUnsupportedType() {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let sql = gen.generateDrop(for: tableId)
        #expect(sql.contains("-- unsupported drop for"))
    }

    @Test("Wrong metadata type throws error")
    func wrongMetadataThrows() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let metadata = EnumMetadata(id: id, labels: ["a"])
        #expect(throws: PGSchemaEvoError.self) {
            try gen.generateCreate(from: metadata)
        }
    }

    @Test("Generate DROP SCHEMA includes CASCADE")
    func dropSchemaHasCascade() {
        let id = ObjectIdentifier(type: .schema, name: "analytics")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP SCHEMA IF EXISTS \"analytics\" CASCADE;")
    }

    @Test("Generate DROP ROLE has correct format")
    func dropRoleFormat() {
        let id = ObjectIdentifier(type: .role, name: "app_user")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP ROLE IF EXISTS \"app_user\";")
    }

    @Test("Generate DROP EXTENSION includes CASCADE")
    func dropExtensionHasCascade() {
        let id = ObjectIdentifier(type: .extension, name: "uuid-ossp")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP EXTENSION IF EXISTS \"uuid-ossp\" CASCADE;")
    }

    @Test("Role name with single quotes is escaped in SQL string")
    func roleNameEscaping() throws {
        let id = ObjectIdentifier(type: .role, name: "o'brien")
        let metadata = RoleMetadata(id: id, canLogin: true)
        let sql = try gen.generateCreate(from: metadata)
        // The rolname check in the DO block should have escaped quotes
        #expect(sql.contains("rolname = 'o''brien'"))
    }

    @Test("Extension version with single quotes is escaped")
    func extensionVersionEscaping() throws {
        let id = ObjectIdentifier(type: .extension, name: "test_ext")
        let metadata = ExtensionMetadata(id: id, version: "1.0'beta")
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("VERSION '1.0''beta'"))
    }
}
