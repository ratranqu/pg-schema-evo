import Testing
@testable import PGSchemaEvoCore

@Suite("UpsertSQLGenerator Tests")
struct UpsertSQLGeneratorTests {
    let generator = UpsertSQLGenerator()

    // MARK: - UPSERT SQL

    @Test("UPSERT with single PK column")
    func upsertSinglePK() {
        let columns = [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "name", dataType: "text", isNullable: false, ordinalPosition: 2),
            ColumnInfo(name: "email", dataType: "text", isNullable: true, ordinalPosition: 3),
        ]

        let sql = generator.generateUpsertSQL(
            table: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            columns: columns,
            pkColumns: ["id"],
            tempTableName: "_sync_tmp_public_users"
        )

        #expect(sql.contains("INSERT INTO \"public\".\"users\""))
        #expect(sql.contains("SELECT * FROM \"_sync_tmp_public_users\""))
        #expect(sql.contains("ON CONFLICT (\"id\")"))
        #expect(sql.contains("DO UPDATE SET"))
        #expect(sql.contains("\"name\" = EXCLUDED.\"name\""))
        #expect(sql.contains("\"email\" = EXCLUDED.\"email\""))
        // PK column should NOT be in SET clause
        #expect(!sql.contains("\"id\" = EXCLUDED.\"id\""))
    }

    @Test("UPSERT with composite PK")
    func upsertCompositePK() {
        let columns = [
            ColumnInfo(name: "tenant_id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "user_id", dataType: "integer", isNullable: false, ordinalPosition: 2),
            ColumnInfo(name: "role", dataType: "text", isNullable: false, ordinalPosition: 3),
        ]

        let sql = generator.generateUpsertSQL(
            table: ObjectIdentifier(type: .table, schema: "public", name: "user_roles"),
            columns: columns,
            pkColumns: ["tenant_id", "user_id"],
            tempTableName: "_sync_tmp"
        )

        #expect(sql.contains("ON CONFLICT (\"tenant_id\", \"user_id\")"))
        #expect(sql.contains("\"role\" = EXCLUDED.\"role\""))
        #expect(!sql.contains("\"tenant_id\" = EXCLUDED"))
        #expect(!sql.contains("\"user_id\" = EXCLUDED"))
    }

    @Test("UPSERT with all-PK table uses DO NOTHING")
    func upsertAllPK() {
        let columns = [
            ColumnInfo(name: "tag", dataType: "text", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "item_id", dataType: "integer", isNullable: false, ordinalPosition: 2),
        ]

        let sql = generator.generateUpsertSQL(
            table: ObjectIdentifier(type: .table, schema: "public", name: "tags"),
            columns: columns,
            pkColumns: ["tag", "item_id"],
            tempTableName: "_sync_tmp"
        )

        #expect(sql.contains("DO NOTHING"))
        #expect(!sql.contains("DO UPDATE"))
    }

    // MARK: - DELETE orphans SQL

    @Test("Delete orphans with single PK")
    func deleteOrphansSinglePK() {
        let sql = generator.generateDeleteOrphansSQL(
            table: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            pkColumns: ["id"],
            deleteTempTableName: "_sync_del_public_users"
        )

        #expect(sql.contains("DELETE FROM \"public\".\"users\" t"))
        #expect(sql.contains("WHERE NOT EXISTS"))
        #expect(sql.contains("s.\"id\" = t.\"id\""))
    }

    @Test("Delete orphans with composite PK")
    func deleteOrphansCompositePK() {
        let sql = generator.generateDeleteOrphansSQL(
            table: ObjectIdentifier(type: .table, schema: "public", name: "user_roles"),
            pkColumns: ["tenant_id", "user_id"],
            deleteTempTableName: "_sync_del"
        )

        #expect(sql.contains("DELETE FROM"))
        #expect(sql.contains("s.\"tenant_id\" = t.\"tenant_id\""))
        #expect(sql.contains("s.\"user_id\" = t.\"user_id\""))
    }

    // MARK: - MAX tracking query

    @Test("MAX tracking query for timestamp column")
    func maxTrackingTimestamp() {
        let sql = generator.generateMaxTrackingQuery(
            table: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            trackingColumn: "updated_at"
        )

        #expect(sql == "SELECT MAX(\"updated_at\")::text FROM \"public\".\"orders\"")
    }

    @Test("MAX tracking query for ID column")
    func maxTrackingID() {
        let sql = generator.generateMaxTrackingQuery(
            table: ObjectIdentifier(type: .table, schema: "analytics", name: "events"),
            trackingColumn: "event_id"
        )

        #expect(sql == "SELECT MAX(\"event_id\")::text FROM \"analytics\".\"events\"")
    }

    // MARK: - Incremental COPY command

    @Test("Incremental COPY command")
    func incrementalCopy() {
        let cmd = generator.generateIncrementalCopyCommand(
            table: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            trackingColumn: "updated_at",
            lastValue: "2026-03-30T12:00:00Z"
        )

        #expect(cmd.contains("\\copy"))
        #expect(cmd.contains("WHERE \"updated_at\" > '2026-03-30T12:00:00Z'"))
        #expect(cmd.contains("ORDER BY \"updated_at\""))
        #expect(cmd.contains("FORMAT csv, HEADER"))
    }

    @Test("Incremental COPY command escapes single quotes in value")
    func incrementalCopyEscapesQuotes() {
        let cmd = generator.generateIncrementalCopyCommand(
            table: ObjectIdentifier(type: .table, schema: "public", name: "items"),
            trackingColumn: "label",
            lastValue: "it's"
        )

        #expect(cmd.contains("'it''s'"))
    }

    // MARK: - PK export command

    @Test("PK export command single column")
    func pkExportSingle() {
        let cmd = generator.generatePKExportCommand(
            table: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            pkColumns: ["id"]
        )

        #expect(cmd.contains("SELECT \"id\" FROM"))
        #expect(cmd.contains("FORMAT csv, HEADER"))
    }

    @Test("PK export command composite PK")
    func pkExportComposite() {
        let cmd = generator.generatePKExportCommand(
            table: ObjectIdentifier(type: .table, schema: "public", name: "user_roles"),
            pkColumns: ["tenant_id", "user_id"]
        )

        #expect(cmd.contains("SELECT \"tenant_id\", \"user_id\""))
    }

    // MARK: - Full table sync script

    @Test("Build table sync script without deletes")
    func tableSyncScriptNoDeletes() {
        let columns = [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "value", dataType: "text", isNullable: true, ordinalPosition: 2),
        ]

        let csv = "id,value\n1,hello\n2,world\n"

        let script = generator.buildTableSyncScript(
            table: ObjectIdentifier(type: .table, schema: "public", name: "items"),
            columns: columns,
            pkColumns: ["id"],
            csvData: csv,
            detectDeletes: false,
            deletePKData: nil
        )

        #expect(script.contains("BEGIN;"))
        #expect(script.contains("CREATE TEMP TABLE"))
        #expect(script.contains("LIKE \"public\".\"items\" INCLUDING DEFAULTS"))
        #expect(script.contains("ON COMMIT DROP"))
        #expect(script.contains("COPY"))
        #expect(script.contains("FROM STDIN WITH (FORMAT csv, HEADER)"))
        #expect(script.contains("1,hello"))
        #expect(script.contains("\\."))
        #expect(script.contains("INSERT INTO"))
        #expect(script.contains("ON CONFLICT"))
        #expect(script.contains("COMMIT;"))
        // No delete section
        #expect(!script.contains("_sync_del_"))
    }

    @Test("Build table sync script with delete detection")
    func tableSyncScriptWithDeletes() {
        let columns = [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "value", dataType: "text", isNullable: true, ordinalPosition: 2),
        ]

        let csv = "id,value\n1,hello\n"
        let pkData = "id\n1\n2\n3\n"

        let script = generator.buildTableSyncScript(
            table: ObjectIdentifier(type: .table, schema: "public", name: "items"),
            columns: columns,
            pkColumns: ["id"],
            csvData: csv,
            detectDeletes: true,
            deletePKData: pkData
        )

        #expect(script.contains("_sync_del_"))
        #expect(script.contains("DELETE FROM"))
        #expect(script.contains("NOT EXISTS"))
    }
}
