import Testing
@testable import PGSchemaEvoCore

@Suite("SequenceSQLGenerator Tests")
struct SequenceSQLGeneratorTests {
    let gen = SequenceSQLGenerator()

    @Test("Generate CREATE SEQUENCE with all options")
    func createSequence() throws {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_seq")
        let metadata = SequenceMetadata(
            id: id,
            dataType: "bigint",
            startValue: 1000,
            increment: 1,
            minValue: 1,
            maxValue: 9999999,
            cacheSize: 10,
            isCycled: false
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE SEQUENCE"))
        #expect(sql.contains("\"public\".\"invoice_seq\""))
        #expect(sql.contains("START WITH 1000"))
        #expect(sql.contains("INCREMENT BY 1"))
        #expect(sql.contains("CACHE 10"))
        #expect(sql.contains("NO CYCLE"))
    }

    @Test("Generate SEQUENCE with CYCLE option")
    func createCyclingSequence() throws {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "cycle_seq")
        let metadata = SequenceMetadata(
            id: id,
            isCycled: true
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CYCLE"))
        #expect(!sql.contains("NO CYCLE"))
    }

    @Test("Generate SEQUENCE with OWNED BY")
    func createOwnedSequence() throws {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "users_id_seq")
        let metadata = SequenceMetadata(
            id: id,
            ownedByColumn: "public.users.id"
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("ALTER SEQUENCE"))
        #expect(sql.contains("OWNED BY public.users.id"))
    }

    @Test("Generate DROP SEQUENCE")
    func dropSequence() {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "test_seq")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP SEQUENCE IF EXISTS"))
        #expect(sql.contains("CASCADE"))
    }
}
