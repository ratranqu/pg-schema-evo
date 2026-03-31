import Testing
@testable import PGSchemaEvoCore

@Suite("ScriptRenderer Tests")
struct ScriptRendererTests {
    let renderer = ScriptRenderer()

    func makeJob(dryRun: Bool = true) -> CloneJob {
        CloneJob(
            source: ConnectionConfig(host: "source", database: "srcdb", username: "user", password: "pass"),
            target: ConnectionConfig(host: "target", database: "tgtdb", username: "admin", password: "secret"),
            objects: [],
            dryRun: dryRun
        )
    }

    @Test("Renders bash script header")
    func scriptHeader() {
        let job = makeJob()
        let script = renderer.render(job: job, steps: [])
        #expect(script.contains("#!/usr/bin/env bash"))
        #expect(script.contains("set -euo pipefail"))
        #expect(script.contains("TARGET_DSN="))
        #expect(script.contains("SOURCE_DSN="))
    }

    @Test("Renders CREATE step with psql heredoc")
    func createStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .createObject(sql: "CREATE TABLE \"public\".\"users\" (\n    \"id\" integer\n);", id: id),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("Create table: table:public.users"))
        #expect(script.contains("psql"))
        #expect(script.contains("EOSQL"))
        #expect(script.contains("CREATE TABLE"))
    }

    @Test("Renders DROP step")
    func dropStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP TABLE IF EXISTS"))
    }

    @Test("Renders COPY data step")
    func copyDataStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [.copyData(id: id, method: .copy, estimatedSize: 5_000_000)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("\\copy"))
        #expect(script.contains("FORMAT csv"))
        #expect(script.contains("4.8 MB"))
    }

    @Test("Renders pg_dump data step for large tables")
    func pgDumpDataStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "big_table")
        let steps: [CloneStep] = [.copyData(id: id, method: .pgDump, estimatedSize: 500_000_000)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("pg_dump"))
        #expect(script.contains("pg_restore"))
        #expect(script.contains("476.8 MB"))
    }

    @Test("Renders permission GRANT step")
    func grantStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .grantPermissions(sql: "GRANT SELECT ON TABLE \"public\".\"users\" TO \"reader\";", id: id),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("Permissions: table:public.users"))
        #expect(script.contains("GRANT SELECT"))
    }

    @Test("Masks password in DSN comments")
    func maskedPasswordInComments() {
        let job = makeJob()
        let script = renderer.render(job: job, steps: [])
        // Comments should show masked password
        #expect(script.contains("****"))
        // But TARGET_DSN should have real password for execution
        #expect(script.contains("secret"))
    }

    // MARK: - Additional Coverage

    @Test("Renders refreshMaterializedView step")
    func refreshMaterializedViewStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_stats")
        let steps: [CloneStep] = [.refreshMaterializedView(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("Refresh materialized view"))
        #expect(script.contains("REFRESH MATERIALIZED VIEW"))
        #expect(script.contains("\"analytics\".\"daily_stats\""))
        #expect(script.contains("EOSQL"))
    }

    @Test("Renders enableRLS step")
    func enableRLSStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let sql = "ALTER TABLE \"public\".\"secrets\" ENABLE ROW LEVEL SECURITY;"
        let steps: [CloneStep] = [.enableRLS(sql: sql, id: id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("Enable RLS"))
        #expect(script.contains("ENABLE ROW LEVEL SECURITY"))
        #expect(script.contains("EOSQL"))
    }

    @Test("Renders attachPartition step")
    func attachPartitionStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events_2024")
        let sql = "ALTER TABLE \"public\".\"events\" ATTACH PARTITION \"public\".\"events_2024\" FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');"
        let steps: [CloneStep] = [.attachPartition(sql: sql, id: id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("Attach partition"))
        #expect(script.contains("ATTACH PARTITION"))
        #expect(script.contains("EOSQL"))
    }

    @Test("Renders copyData with WHERE clause and row limit")
    func copyDataWithWhereAndLimit() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: 1_000_000, whereClause: "created_at > '2024-01-01'", rowLimit: 500),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("WHERE created_at > '2024-01-01'"))
        #expect(script.contains("LIMIT 500"))
        #expect(script.contains("SELECT * FROM"))
    }

    @Test("Renders copyData via pgDump method with section header")
    func copyDataPgDumpHeader() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "warehouse", name: "fact_sales")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .pgDump, estimatedSize: 2_000_000_000),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("pg_dump"))
        #expect(script.contains("pg_restore"))
        #expect(script.contains("--data-only"))
        #expect(script.contains("--no-owner"))
        #expect(script.contains("method: pgdump"))
        #expect(script.contains("1.9 GB"))
    }

    @Test("formatBytes renders GB range correctly")
    func formatBytesGB() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        // 2.5 GB
        let steps: [CloneStep] = [.copyData(id: id, method: .copy, estimatedSize: 2_684_354_560)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("2.5 GB"))
    }

    @Test("formatBytes renders KB range correctly")
    func formatBytesKB() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        // 512 KB
        let steps: [CloneStep] = [.copyData(id: id, method: .copy, estimatedSize: 524_288)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("512.0 KB"))
    }

    @Test("formatBytes renders bytes range correctly")
    func formatBytesB() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        // 500 bytes
        let steps: [CloneStep] = [.copyData(id: id, method: .copy, estimatedSize: 500)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("500 B"))
    }

    @Test("Renders DROP for view object type")
    func dropView() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP VIEW IF EXISTS"))
        #expect(script.contains("\"public\".\"active_users\""))
        #expect(script.contains("CASCADE"))
    }

    @Test("Renders DROP for materialized view")
    func dropMaterializedView() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv_stats")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP MATERIALIZED VIEW IF EXISTS"))
    }

    @Test("Renders DROP for function")
    func dropFunction() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .function, schema: "public", name: "calc")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP FUNCTION IF EXISTS"))
    }

    @Test("Renders DROP for enum type")
    func dropEnum() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP TYPE IF EXISTS"))
    }

    @Test("Renders DROP for sequence")
    func dropSequence() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "users_id_seq")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP SEQUENCE IF EXISTS"))
    }

    @Test("Renders DROP for composite type")
    func dropCompositeType() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP TYPE IF EXISTS"))
    }

    @Test("Renders DROP for procedure")
    func dropProcedure() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .procedure, schema: "public", name: "cleanup")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP PROCEDURE IF EXISTS"))
    }

    @Test("Renders DROP for foreign table")
    func dropForeignTable() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .foreignTable, schema: "public", name: "remote_data")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP FOREIGN TABLE IF EXISTS"))
    }

    @Test("copyData with unknown estimated size shows 'unknown size'")
    func copyDataUnknownSize() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let steps: [CloneStep] = [.copyData(id: id, method: .copy, estimatedSize: nil)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("unknown size"))
    }

    // MARK: - sqlObjectType coverage for all object types

    @Test("DROP renders AGGREGATE for aggregate type")
    func dropAggregate() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .aggregate, schema: "public", name: "array_agg_custom")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP AGGREGATE IF EXISTS"))
    }

    @Test("DROP renders OPERATOR for operator type")
    func dropOperator() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .operator, schema: "public", name: "===")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP OPERATOR IF EXISTS"))
    }

    @Test("DROP renders FOREIGN DATA WRAPPER for fdw type")
    func dropForeignDataWrapper() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .foreignDataWrapper, name: "postgres_fdw")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP FOREIGN DATA WRAPPER IF EXISTS"))
    }

    @Test("DROP renders ROLE for role type")
    func dropRole() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .role, name: "readonly")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP ROLE IF EXISTS"))
    }

    @Test("DROP renders SCHEMA for schema type")
    func dropSchema() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .schema, name: "analytics")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP SCHEMA IF EXISTS"))
    }

    @Test("DROP renders EXTENSION for extension type")
    func dropExtension() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .extension, name: "uuid-ossp")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("DROP EXTENSION IF EXISTS"))
    }

    // MARK: - copyViaPsql edge cases

    @Test("copyViaPsql with WHERE clause only (no LIMIT)")
    func copyDataWithWhereOnly() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: 1000, whereClause: "status = 'active'"),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("SELECT * FROM"))
        #expect(script.contains("WHERE status = 'active'"))
        #expect(!script.contains("LIMIT"))
    }

    @Test("copyViaPsql with LIMIT only (no WHERE clause)")
    func copyDataWithLimitOnly() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: 1000, rowLimit: 100),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("SELECT * FROM"))
        #expect(script.contains("LIMIT 100"))
        #expect(!script.contains("WHERE"))
    }

    @Test("copyViaPsql without WHERE or LIMIT uses simple COPY")
    func copyDataSimpleCopy() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: 1000),
        ]
        let script = renderer.render(job: job, steps: steps)
        // Simple copy should NOT use SELECT * FROM subquery
        #expect(!script.contains("SELECT * FROM"))
        #expect(script.contains("\\copy"))
        #expect(script.contains("\"public\".\"users\""))
    }

    @Test("copyViaPsql with auto method renders like copy method")
    func copyDataAutoMethod() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "data")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .auto, estimatedSize: 500),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("\\copy"))
        #expect(!script.contains("pg_dump"))
    }

    // MARK: - copyViaPgDump rendering details

    @Test("copyViaPgDump includes table name and correct flags")
    func copyViaPgDumpDetails() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "warehouse", name: "inventory")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .pgDump, estimatedSize: 2_000_000_000),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("--format=custom"))
        #expect(script.contains("--data-only"))
        #expect(script.contains("--table=\"warehouse\".\"inventory\""))
        #expect(script.contains("$SOURCE_DSN"))
        #expect(script.contains("pg_restore"))
        #expect(script.contains("--no-owner"))
        #expect(script.contains("--dbname=\"$TARGET_DSN\""))
    }

    // MARK: - Multiple steps numbering

    @Test("Multiple steps are numbered sequentially in section headers")
    func multipleStepsNumbering() {
        let job = makeJob()
        let id1 = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let id2 = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let steps: [CloneStep] = [
            .dropObject(id1),
            .createObject(sql: "CREATE TABLE a();", id: id1),
            .dropObject(id2),
            .createObject(sql: "CREATE TABLE b();", id: id2),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("# 1."))
        #expect(script.contains("# 2."))
        #expect(script.contains("# 3."))
        #expect(script.contains("# 4."))
    }

    // MARK: - Section header format

    @Test("copyData section header includes method and WHERE/LIMIT info")
    func copyDataSectionHeaderDetails() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .pgDump, estimatedSize: 1_000_000, whereClause: "year > 2020", rowLimit: 1000),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("method: pgdump"))
        #expect(script.contains("WHERE year > 2020"))
        #expect(script.contains("LIMIT 1000"))
    }

    @Test("Renders ALTER step with psql heredoc")
    func alterStep() {
        let job = makeJob()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .alterObject(sql: "ALTER TABLE \"public\".\"users\" ADD COLUMN \"email\" text;", id: id),
        ]
        let script = renderer.render(job: job, steps: steps)
        #expect(script.contains("Alter table: table:public.users"))
        #expect(script.contains("ALTER TABLE"))
        #expect(script.contains("ADD COLUMN"))
        #expect(script.contains("psql"))
    }
}
