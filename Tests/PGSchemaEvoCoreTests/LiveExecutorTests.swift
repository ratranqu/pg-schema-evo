import Testing
import Logging
@testable import PGSchemaEvoCore

@Suite("LiveExecutor Transaction Script Tests")
struct LiveExecutorTests {

    private func makeExecutor() -> LiveExecutor {
        LiveExecutor(logger: Logger(label: "test"))
    }

    private func qn(schema: String, name: String) -> String {
        "\"\(schema)\".\"\(name)\""
    }

    // MARK: - buildTransactionScript

    @Test("Empty steps produce BEGIN/COMMIT only")
    func emptyScript() {
        let executor = makeExecutor()
        let script = executor.buildTransactionScript(steps: [], prefetchedData: [:])
        #expect(script == "BEGIN;\n\nCOMMIT;\n")
    }

    @Test("Single CREATE step is wrapped in transaction")
    func createStep() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let sql = "CREATE TABLE \(qn(schema: "public", name: "users")) (id int);"
        let steps: [CloneStep] = [
            .createObject(sql: sql, id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.hasPrefix("BEGIN;\n"))
        #expect(script.hasSuffix("COMMIT;\n"))
        #expect(script.contains(sql))
    }

    @Test("DROP step generates correct SQL in script")
    func dropStep() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP TABLE IF EXISTS \(qn(schema: "public", name: "users")) CASCADE;"))
    }

    @Test("COPY data is inlined with COPY FROM STDIN")
    func copyDataInlined() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: nil),
        ]
        let csvData = "id,name\n1,Alice\n2,Bob\n"
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [0: csvData])
        #expect(script.contains("COPY \(qn(schema: "public", name: "users")) FROM STDIN WITH (FORMAT csv, HEADER);"))
        #expect(script.contains("1,Alice"))
        #expect(script.contains("2,Bob"))
        #expect(script.contains("\\.\n"))
    }

    @Test("pgDump data is inlined as raw SQL")
    func pgDumpDataInlined() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .pgDump, estimatedSize: nil),
        ]
        let pgDumpOutput = "COPY public.users (id, name) FROM stdin;\n1\tAlice\n2\tBob\n\\.\n"
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [0: pgDumpOutput])
        // pgDump output is included as-is (it already has COPY statements)
        #expect(script.contains("COPY public.users (id, name) FROM stdin;"))
        #expect(script.contains("1\tAlice"))
        // Should NOT wrap in additional COPY FROM STDIN
        #expect(!script.contains("FROM STDIN WITH (FORMAT csv, HEADER);"))
    }

    @Test("Empty COPY data is skipped")
    func emptyCopyDataSkipped() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: nil),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [0: ""])
        #expect(!script.contains("FROM STDIN"))
    }

    @Test("Missing prefetched data is handled")
    func missingPrefetchedData() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: nil),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(!script.contains("FROM STDIN"))
    }

    @Test("GRANT permissions step is included")
    func grantPermissions() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .grantPermissions(sql: "GRANT SELECT ON public.users TO readonly;", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("GRANT SELECT ON public.users TO readonly;"))
    }

    @Test("REFRESH materialized view step is included")
    func refreshMatview() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv_stats")
        let steps: [CloneStep] = [.refreshMaterializedView(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("REFRESH MATERIALIZED VIEW \(qn(schema: "public", name: "mv_stats"));"))
    }

    @Test("Enable RLS step is included")
    func enableRLS() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .enableRLS(sql: "ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;"))
    }

    @Test("Attach partition step is included")
    func attachPartition() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders_2024")
        let sql = "ALTER TABLE public.orders ATTACH PARTITION public.orders_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');"
        let steps: [CloneStep] = [.attachPartition(sql: sql, id: id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("ATTACH PARTITION"))
    }

    @Test("Multiple steps are ordered correctly")
    func multipleStepsOrdering() {
        let executor = makeExecutor()
        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .createObject(sql: "CREATE TYPE public.status AS ENUM ('active');", id: enumId),
            .createObject(sql: "CREATE TABLE public.users (id int, status public.status);", id: tableId),
            .copyData(id: tableId, method: .copy, estimatedSize: 100),
            .grantPermissions(sql: "GRANT SELECT ON public.users TO app;", id: tableId),
        ]
        let csvData = "id,status\n1,active\n"
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [2: csvData])

        // Verify ordering: BEGIN → CREATE TYPE → CREATE TABLE → COPY → GRANT → COMMIT
        let beginIdx = script.range(of: "BEGIN;")!.lowerBound
        let createTypeIdx = script.range(of: "CREATE TYPE")!.lowerBound
        let createTableIdx = script.range(of: "CREATE TABLE")!.lowerBound
        let copyIdx = script.range(of: "FROM STDIN WITH (FORMAT csv, HEADER);")!.lowerBound
        let grantIdx = script.range(of: "GRANT SELECT")!.lowerBound
        let commitIdx = script.range(of: "COMMIT;")!.lowerBound

        #expect(beginIdx < createTypeIdx)
        #expect(createTypeIdx < createTableIdx)
        #expect(createTableIdx < copyIdx)
        #expect(copyIdx < grantIdx)
        #expect(grantIdx < commitIdx)
    }

    @Test("Step comments are included")
    func stepComments() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let steps: [CloneStep] = [
            .createObject(sql: "CREATE TABLE t (id int);", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("-- Step 1:"))
    }

    @Test("COPY data without trailing newline gets one added")
    func copyDataTrailingNewline() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .copy, estimatedSize: nil),
        ]
        // Data without trailing newline
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [0: "id\n1"])
        // Should have newline before \. terminator
        #expect(script.contains("1\n\\.\n"))
    }

    @Test("DROP function includes signature")
    func dropFunctionSignature() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .function, schema: "public", name: "my_func", signature: "(integer, text)")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP FUNCTION IF EXISTS \(qn(schema: "public", name: "my_func"))(integer, text) CASCADE;"))
    }

    @Test("ALTER step is included in transaction script")
    func alterStep() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let alterSQL = "ALTER TABLE \(qn(schema: "public", name: "users")) ADD COLUMN email text;"
        let steps: [CloneStep] = [.alterObject(sql: alterSQL, id: id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("BEGIN;"))
        #expect(script.contains("ALTER TABLE"))
        #expect(script.contains("ADD COLUMN email text;"))
        #expect(script.contains("COMMIT;"))
    }

    @Test("ALTER step description")
    func alterStepDescription() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "my_seq")
        let steps: [CloneStep] = [
            .alterObject(sql: "ALTER SEQUENCE \(qn(schema: "public", name: "my_seq")) INCREMENT BY 5;", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("Alter"))
        #expect(script.contains("INCREMENT BY 5"))
    }
}
