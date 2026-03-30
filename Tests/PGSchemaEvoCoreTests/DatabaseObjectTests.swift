import Testing
@testable import PGSchemaEvoCore

@Suite("DatabaseObject Tests")
struct DatabaseObjectTests {

    // MARK: - ObjectIdentifier

    @Test("ObjectIdentifier description includes type and qualified name")
    func identifierDescription() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        #expect(id.description == "table:public.users")
    }

    @Test("ObjectIdentifier description for schema-less objects")
    func identifierNoSchema() {
        let id = ObjectIdentifier(type: .role, name: "admin")
        #expect(id.description == "role:admin")
    }

    @Test("ObjectIdentifier description with function signature")
    func identifierWithSignature() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "calculate", signature: "(integer)")
        #expect(id.description == "function:public.calculate(integer)")
    }

    @Test("ObjectIdentifier qualifiedName quotes identifiers")
    func qualifiedNameQuoting() {
        let id = ObjectIdentifier(type: .table, schema: "my schema", name: "my table")
        #expect(id.qualifiedName == "\"my schema\".\"my table\"")
    }

    @Test("ObjectIdentifier equality")
    func identifierEquality() {
        let a = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "users")
        #expect(a == b)
    }

    @Test("ObjectIdentifier inequality by type")
    func identifierInequalityByType() {
        let a = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let b = ObjectIdentifier(type: .view, schema: "public", name: "users")
        #expect(a != b)
    }

    // MARK: - ObjectType properties

    @Test("Table supports data")
    func tableSupportsData() {
        #expect(ObjectType.table.supportsData)
    }

    @Test("View does not support data")
    func viewNoData() {
        #expect(!ObjectType.view.supportsData)
    }

    @Test("Role is not schema-scoped")
    func roleNotSchemaScoped() {
        #expect(!ObjectType.role.isSchemaScoped)
    }

    @Test("Table is schema-scoped")
    func tableSchemaScoped() {
        #expect(ObjectType.table.isSchemaScoped)
    }

    // MARK: - parseObjectSpecifier

    @Test("Parse table specifier with schema")
    func parseTableWithSchema() throws {
        let id = try parseObjectSpecifier("table:myschema.orders")
        #expect(id.type == .table)
        #expect(id.schema == "myschema")
        #expect(id.name == "orders")
    }

    @Test("Parse table specifier without schema defaults to public")
    func parseTableDefaultSchema() throws {
        let id = try parseObjectSpecifier("table:orders")
        #expect(id.type == .table)
        #expect(id.schema == "public")
        #expect(id.name == "orders")
    }

    @Test("Parse function specifier with signature")
    func parseFunctionWithSignature() throws {
        let id = try parseObjectSpecifier("function:public.calculate_total(integer)")
        #expect(id.type == .function)
        #expect(id.schema == "public")
        #expect(id.name == "calculate_total")
        #expect(id.signature == "(integer)")
    }

    @Test("Parse role specifier (no schema)")
    func parseRoleNoSchema() throws {
        let id = try parseObjectSpecifier("role:admin")
        #expect(id.type == .role)
        #expect(id.schema == nil)
        #expect(id.name == "admin")
    }

    @Test("Parse materialized view")
    func parseMatview() throws {
        let id = try parseObjectSpecifier("matview:analytics.daily_stats")
        #expect(id.type == .materializedView)
        #expect(id.schema == "analytics")
        #expect(id.name == "daily_stats")
    }

    @Test("Reject unknown object type")
    func rejectUnknownType() {
        #expect(throws: PGSchemaEvoError.self) {
            try parseObjectSpecifier("bogus:public.foo")
        }
    }

    @Test("Reject missing colon")
    func rejectMissingColon() {
        #expect(throws: PGSchemaEvoError.self) {
            try parseObjectSpecifier("table.public.foo")
        }
    }
}
