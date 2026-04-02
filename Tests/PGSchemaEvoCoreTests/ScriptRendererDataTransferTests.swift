import Testing
@testable import PGSchemaEvoCore

@Suite("ScriptRenderer Data Transfer Tests")
struct ScriptRendererDataTransferTests {
    let renderer = ScriptRenderer()

    func makeJob(parallel: Int = 0) -> CloneJob {
        CloneJob(
            source: ConnectionConfig(host: "source", database: "srcdb", username: "user", password: "pass"),
            target: ConnectionConfig(host: "target", database: "tgtdb", username: "admin", password: "secret"),
            objects: [],
            dryRun: true,
            parallel: parallel
        )
    }

    // MARK: - Parallel data transfers section

    @Test("Render with data transfers includes parallel section header")
    func parallelSectionHeader() {
        let job = makeJob(parallel: 4)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: 5000),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("Data Transfers (parallel: 4)"))
    }

    @Test("Render with data transfers via copy method")
    func dataTransferCopy() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: 1000),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("\\copy"))
        #expect(script.contains("FORMAT csv"))
    }

    @Test("Render with data transfers via pgDump method")
    func dataTransferPgDump() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "big_table")
        let transfers = [
            DataTransferTask(id: id, method: .pgDump, estimatedSize: 500_000_000),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("pg_dump"))
        #expect(script.contains("pg_restore"))
    }

    @Test("Render with data transfers via auto method uses copy")
    func dataTransferAuto() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let transfers = [
            DataTransferTask(id: id, method: .auto, estimatedSize: 100),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("\\copy"))
    }

    @Test("Render with data transfers and WHERE clause")
    func dataTransferWithWhere() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: nil, whereClause: "year > 2023"),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("SELECT * FROM"))
        #expect(script.contains("WHERE year > 2023"))
    }

    @Test("Render with data transfers and row limit")
    func dataTransferWithRowLimit() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: nil, rowLimit: 500),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("LIMIT 500"))
    }

    @Test("Render with unknown estimated size shows 'unknown size'")
    func dataTransferUnknownSize() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: nil),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        #expect(script.contains("unknown size"))
    }

    @Test("Render with multiple data transfers numbers them sequentially")
    func dataTransferNumbering() {
        let job = makeJob(parallel: 4)
        let id1 = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let id2 = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let steps: [CloneStep] = [
            .createObject(sql: "CREATE TABLE a();", id: id1),
        ]
        let transfers = [
            DataTransferTask(id: id1, method: .copy, estimatedSize: nil),
            DataTransferTask(id: id2, method: .copy, estimatedSize: nil),
        ]
        let script = renderer.render(job: job, steps: steps, dataTransfers: transfers)
        // Steps start at 1, transfers continue from steps.count + 1
        #expect(script.contains("# 1."))
        #expect(script.contains("# 2."))
        #expect(script.contains("# 3."))
    }

    @Test("Render with empty data transfers does not include parallel section")
    func emptyDataTransfers() {
        let job = makeJob(parallel: 4)
        let script = renderer.render(job: job, steps: [], dataTransfers: [])
        #expect(!script.contains("Data Transfers"))
    }

    @Test("Render with steps and data transfers includes both sections")
    func stepsAndTransfers() {
        let job = makeJob(parallel: 2)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .createObject(sql: "CREATE TABLE users (id int);", id: id),
        ]
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: 1000),
        ]
        let script = renderer.render(job: job, steps: steps, dataTransfers: transfers)
        #expect(script.contains("Create table"))
        #expect(script.contains("Data Transfers"))
    }

    // MARK: - auto detect concurrency in parallel section header

    @Test("Auto parallel (0) uses autoDetectConcurrency for header")
    func autoParallelHeader() {
        let job = makeJob(parallel: 0)
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let transfers = [
            DataTransferTask(id: id, method: .copy, estimatedSize: nil),
        ]
        let script = renderer.render(job: job, steps: [], dataTransfers: transfers)
        let auto = ParallelDataTransfer.autoDetectConcurrency()
        #expect(script.contains("parallel: \(auto)"))
    }
}
