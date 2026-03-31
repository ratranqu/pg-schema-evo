import Testing
@testable import PGSchemaEvoCore

@Suite("FunctionSQLGenerator Tests")
struct FunctionSQLGeneratorTests {
    let gen = FunctionSQLGenerator()

    @Test("Generate CREATE from pg_get_functiondef output")
    func createFunction() throws {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "add_numbers")
        let metadata = FunctionMetadata(
            id: id,
            definition: """
                CREATE OR REPLACE FUNCTION public.add_numbers(a integer, b integer)
                 RETURNS integer
                 LANGUAGE sql
                 IMMUTABLE
                AS $function$SELECT a + b$function$
                """,
            language: "sql",
            returnType: "integer",
            volatility: "IMMUTABLE",
            argumentSignature: "a integer, b integer"
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE OR REPLACE FUNCTION"))
        #expect(sql.contains("public.add_numbers"))
        #expect(sql.hasSuffix(";"))
    }

    @Test("Generate DROP FUNCTION with signature")
    func dropFunction() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "add_numbers", signature: "(integer, integer)")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP FUNCTION IF EXISTS"))
        #expect(sql.contains("(integer, integer)"))
        #expect(sql.contains("CASCADE"))
    }

    @Test("Generate DROP PROCEDURE")
    func dropProcedure() {
        let id = ObjectIdentifier(type: .procedure, schema: "public", name: "do_work", signature: "()")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP PROCEDURE IF EXISTS"))
    }

    @Test("Definition already ending with semicolon is not doubled")
    func definitionWithSemicolon() throws {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "noop")
        let metadata = FunctionMetadata(
            id: id,
            definition: "CREATE FUNCTION public.noop() RETURNS void LANGUAGE sql AS '';",
            language: "sql",
            returnType: "void",
            volatility: "VOLATILE",
            argumentSignature: ""
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.hasSuffix(";"))
        #expect(!sql.hasSuffix(";;"))
    }

    @Test("Wrong metadata type throws error")
    func wrongMetadata() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "f")
        let metadata = EnumMetadata(id: id, labels: [])
        #expect(throws: PGSchemaEvoError.self) {
            try gen.generateCreate(from: metadata)
        }
    }

    @Test("DROP FUNCTION without explicit signature defaults to ()")
    func dropNoSignature() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "f")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("()"))
    }

    @Test("Definition without trailing semicolon gets one appended")
    func definitionWithoutSemicolon() throws {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "my_func")
        let metadata = FunctionMetadata(
            id: id,
            definition: "CREATE FUNCTION public.my_func() RETURNS void LANGUAGE sql AS ''"
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.hasSuffix(";"))
        #expect(!sql.hasSuffix(";;"))
    }

    @Test("DROP PROCEDURE format with signature")
    func dropProcedureFormat() {
        let id = ObjectIdentifier(type: .procedure, schema: "myschema", name: "cleanup", signature: "(integer)")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP PROCEDURE IF EXISTS \"myschema\".\"cleanup\"(integer) CASCADE;")
    }

    @Test("DROP FUNCTION format with signature")
    func dropFunctionFormat() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "add", signature: "(integer, integer)")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP FUNCTION IF EXISTS \"public\".\"add\"(integer, integer) CASCADE;")
    }
}
