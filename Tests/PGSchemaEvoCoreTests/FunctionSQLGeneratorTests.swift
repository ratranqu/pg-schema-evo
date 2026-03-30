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
}
