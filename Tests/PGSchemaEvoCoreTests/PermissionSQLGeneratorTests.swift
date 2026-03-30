import Testing
@testable import PGSchemaEvoCore

@Suite("PermissionSQLGenerator Tests")
struct PermissionSQLGeneratorTests {
    let generator = PermissionSQLGenerator()

    @Test("Generate GRANT for single privilege")
    func singleGrant() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let grants = [PermissionGrant(grantee: "reader", privilege: "SELECT")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT SELECT ON TABLE \"public\".\"users\" TO \"reader\""))
    }

    @Test("Generate GRANT groups privileges by grantee")
    func groupedGrants() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let grants = [
            PermissionGrant(grantee: "writer", privilege: "SELECT"),
            PermissionGrant(grantee: "writer", privilege: "INSERT"),
            PermissionGrant(grantee: "writer", privilege: "UPDATE"),
        ]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT SELECT, INSERT, UPDATE ON TABLE"))
    }

    @Test("No output for empty grants")
    func emptyGrants() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let sql = generator.generateGrants(for: id, grants: [])
        #expect(sql.isEmpty)
    }
}
