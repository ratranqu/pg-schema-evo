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

    @Test("Wrong metadata type throws error")
    func wrongMetadataThrows() {
        let id = ObjectIdentifier(type: .view, schema: "public", name: "v")
        let metadata = EnumMetadata(id: id, labels: ["x"])
        #expect(throws: PGSchemaEvoError.self) {
            try gen.generateCreate(from: metadata)
        }
    }

    @Test("Materialized view without indexes produces no CREATE INDEX")
    func materializedViewNoIndexes() throws {
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv_simple")
        let metadata = MaterializedViewMetadata(
            id: id,
            definition: " SELECT 1 AS val"
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE MATERIALIZED VIEW"))
        #expect(sql.contains("WITH DATA"))
        #expect(!sql.contains("CREATE INDEX"))
    }

    @Test("Materialized view with multiple indexes")
    func materializedViewMultipleIndexes() throws {
        let id = ObjectIdentifier(type: .materializedView, schema: "reporting", name: "mv_report")
        let metadata = MaterializedViewMetadata(
            id: id,
            definition: " SELECT id, name, created_at FROM users",
            indexes: [
                IndexInfo(
                    name: "idx_mv_report_id",
                    definition: "CREATE UNIQUE INDEX idx_mv_report_id ON reporting.mv_report USING btree (id)",
                    isUnique: true,
                    isPrimary: false
                ),
                IndexInfo(
                    name: "idx_mv_report_created_at",
                    definition: "CREATE INDEX idx_mv_report_created_at ON reporting.mv_report USING btree (created_at)",
                    isUnique: false,
                    isPrimary: false
                ),
            ]
        )
        let sql = try gen.generateCreate(from: metadata)
        #expect(sql.contains("CREATE UNIQUE INDEX idx_mv_report_id"))
        #expect(sql.contains("CREATE INDEX idx_mv_report_created_at"))
    }

    @Test("DROP VIEW for non-materialized view type")
    func dropRegularViewFormat() {
        let id = ObjectIdentifier(type: .view, schema: "public", name: "my_view")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP VIEW IF EXISTS \"public\".\"my_view\" CASCADE;")
        #expect(!sql.contains("MATERIALIZED"))
    }

    @Test("DROP MATERIALIZED VIEW format")
    func dropMaterializedViewFormat() {
        let id = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "mv_test")
        let sql = gen.generateDrop(for: id)
        #expect(sql == "DROP MATERIALIZED VIEW IF EXISTS \"analytics\".\"mv_test\" CASCADE;")
    }

    @Test("DROP for table type defaults to VIEW")
    func dropDefaultsToView() {
        // The default branch in generateDrop handles non-materializedView types
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let sql = gen.generateDrop(for: id)
        #expect(sql.contains("DROP VIEW IF EXISTS"))
    }
}
