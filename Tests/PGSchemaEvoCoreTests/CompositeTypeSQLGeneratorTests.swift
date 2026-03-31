import Testing
@testable import PGSchemaEvoCore

@Suite("CompositeTypeSQLGenerator Tests")
struct CompositeTypeSQLGeneratorTests {
    let generator = CompositeTypeSQLGenerator()

    @Test("Generate CREATE TYPE for composite type")
    func generateCreateCompositeType() throws {
        let id = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let metadata = CompositeTypeMetadata(
            id: id,
            attributes: [
                CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
                CompositeTypeAttribute(name: "city", dataType: "text", ordinalPosition: 2),
                CompositeTypeAttribute(name: "state", dataType: "character varying(2)", ordinalPosition: 3),
                CompositeTypeAttribute(name: "zip_code", dataType: "text", ordinalPosition: 4),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)

        #expect(sql.contains("CREATE TYPE"))
        #expect(sql.contains("\"public\".\"address\""))
        #expect(sql.contains("\"street\" text"))
        #expect(sql.contains("\"city\" text"))
        #expect(sql.contains("\"state\" character varying(2)"))
        #expect(sql.contains("\"zip_code\" text"))
    }

    @Test("Generate DROP TYPE for composite type")
    func generateDropCompositeType() {
        let id = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let sql = generator.generateDrop(for: id)

        #expect(sql.contains("DROP TYPE IF EXISTS"))
        #expect(sql.contains("\"public\".\"address\""))
        #expect(sql.contains("CASCADE"))
    }

    @Test("Wrong metadata type throws error")
    func wrongMetadataThrows() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let metadata = EnumMetadata(id: id, labels: ["a"])
        #expect(throws: PGSchemaEvoError.self) {
            try generator.generateCreate(from: metadata)
        }
    }
}
