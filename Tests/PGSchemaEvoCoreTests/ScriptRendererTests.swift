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
}
