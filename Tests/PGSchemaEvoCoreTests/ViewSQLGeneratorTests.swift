import Testing
@testable import PGSchemaEvoCore

@Suite("ViewSQLGenerator Tests")
struct ViewSQLGeneratorTests {
    let gen = ViewSQLGenerator()

    @Test("Generate CREATE VIEW from definition")
    func createView() throws {
        let id = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let metadata = ViewMetadata(
            id: id,
            definition: " SELECT id, username\n   FROM users\n  WHERE active = true"
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE OR REPLACE VIEW"))
        #expect(sql.contains("\"public\".\"active_users\""))
        #expect(sql.contains("SELECT id, username"))
    }

    @Test("Generate CREATE MATERIALIZED VIEW with indexes")
    func createMaterializedView() throws {
        let id = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_summary")
        let metadata = MaterializedViewMetadata(
            id: id,
            definition: " SELECT date_trunc('day', created_at) AS day, count(*) AS cnt\n   FROM orders\n  GROUP BY 1",
            indexes: [
                IndexInfo(
                    name: "idx_daily_summary_day",
                    definition: "CREATE INDEX idx_daily_summary_day ON analytics.daily_summary USING btree (day)",
                    isUnique: false,
                    isPrimary: false
                )
            ]
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE MATERIALIZED VIEW"))
        #expect(sql.contains("\"analytics\".\"daily_summary\""))
        #expect(sql.contains("WITH DATA"))
        #expect(sql.contains("CREATE INDEX idx_daily_summary_day"))
    }

    @Test("Generate DROP VIEW")
    func dropView() {
        let id = ObjectIdentifier(type: .view, schema: "public", name: "test_view")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP VIEW IF EXISTS"))
        #expect(sql.contains("CASCADE"))
    }

    @Test("Generate DROP MATERIALIZED VIEW")
    func dropMaterializedView() {
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "test_matview")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP MATERIALIZED VIEW IF EXISTS"))
    }
}
