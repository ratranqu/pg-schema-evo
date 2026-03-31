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

    @Test("Generate GRANT for SEQUENCE type")
    func grantOnSequence() {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "user_id_seq")
        let grants = [PermissionGrant(grantee: "app_user", privilege: "USAGE")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT USAGE ON SEQUENCE \"public\".\"user_id_seq\" TO \"app_user\""))
    }

    @Test("Generate GRANT for FUNCTION with signature")
    func grantOnFunction() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "calculate", signature: "(integer, integer)")
        let grants = [PermissionGrant(grantee: "app_user", privilege: "EXECUTE")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT EXECUTE ON FUNCTION \"public\".\"calculate\"(integer, integer) TO \"app_user\""))
    }

    @Test("Generate GRANT for PROCEDURE with signature")
    func grantOnProcedure() {
        let id = ObjectIdentifier(type: .procedure, schema: "public", name: "do_work", signature: "(text)")
        let grants = [PermissionGrant(grantee: "worker", privilege: "EXECUTE")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT EXECUTE ON PROCEDURE \"public\".\"do_work\"(text) TO \"worker\""))
    }

    @Test("Generate GRANT for FUNCTION without signature defaults to ()")
    func grantOnFunctionNoSignature() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "noop")
        let grants = [PermissionGrant(grantee: "reader", privilege: "EXECUTE")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("\"public\".\"noop\"()"))
    }

    @Test("Generate GRANT for SCHEMA type")
    func grantOnSchema() {
        let id = ObjectIdentifier(type: .schema, name: "analytics")
        let grants = [PermissionGrant(grantee: "analyst", privilege: "USAGE")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT USAGE ON SCHEMA \"analytics\" TO \"analyst\""))
    }

    @Test("Generate GRANT for VIEW uses TABLE keyword")
    func grantOnView() {
        let id = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let grants = [PermissionGrant(grantee: "reader", privilege: "SELECT")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT SELECT ON TABLE"))
    }

    @Test("Generate GRANT for MATERIALIZED VIEW uses TABLE keyword")
    func grantOnMaterializedView() {
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv_summary")
        let grants = [PermissionGrant(grantee: "reader", privilege: "SELECT")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT SELECT ON TABLE"))
    }

    @Test("Generate GRANT for FOREIGN TABLE uses TABLE keyword")
    func grantOnForeignTable() {
        let id = ObjectIdentifier(type: .foreignTable, schema: "public", name: "remote_data")
        let grants = [PermissionGrant(grantee: "reader", privilege: "SELECT")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT SELECT ON TABLE"))
    }

    @Test("Generate GRANT for unsupported type defaults to TABLE")
    func grantOnDefaultType() {
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let grants = [PermissionGrant(grantee: "reader", privilege: "USAGE")]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("ON TABLE"))
    }

    @Test("Multiple grantees produce separate GRANT statements")
    func multipleGrantees() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "data")
        let grants = [
            PermissionGrant(grantee: "alice", privilege: "SELECT"),
            PermissionGrant(grantee: "bob", privilege: "SELECT"),
            PermissionGrant(grantee: "bob", privilege: "INSERT"),
        ]
        let sql = generator.generateGrants(for: id, grants: grants)
        #expect(sql.contains("GRANT SELECT ON TABLE \"public\".\"data\" TO \"alice\""))
        #expect(sql.contains("GRANT SELECT, INSERT ON TABLE \"public\".\"data\" TO \"bob\""))
    }

    @Test("Grantees are sorted alphabetically")
    func granteeSorting() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let grants = [
            PermissionGrant(grantee: "zeta", privilege: "SELECT"),
            PermissionGrant(grantee: "alpha", privilege: "SELECT"),
        ]
        let sql = generator.generateGrants(for: id, grants: grants)
        let alphaPos = sql.range(of: "\"alpha\"")!.lowerBound
        let zetaPos = sql.range(of: "\"zeta\"")!.lowerBound
        #expect(alphaPos < zetaPos)
    }
}
