import Testing
@testable import PGSchemaEvoCore

@Suite("EnumSQLGenerator Tests")
struct EnumSQLGeneratorTests {
    let gen = EnumSQLGenerator()

    @Test("Generate CREATE TYPE AS ENUM")
    func createEnum() throws {
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "order_status")
        let metadata = EnumMetadata(id: id, labels: ["pending", "processing", "shipped", "delivered"])
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE TYPE"))
        #expect(sql.contains("\"public\".\"order_status\""))
        #expect(sql.contains("AS ENUM"))
        #expect(sql.contains("'pending'"))
        #expect(sql.contains("'delivered'"))
    }

    @Test("Enum labels with single quotes are escaped")
    func escapedLabels() throws {
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "test_enum")
        let metadata = EnumMetadata(id: id, labels: ["it's", "fine"])
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("'it''s'"))
    }

    @Test("Generate DROP TYPE")
    func dropEnum() {
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP TYPE IF EXISTS"))
        #expect(sql.contains("CASCADE"))
    }
}
