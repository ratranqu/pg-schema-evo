import Testing
@testable import PGSchemaEvoCore

@Suite("Retry and Rollback Tests")
struct RetryRollbackTests {

    @Test("CloneJob default retries is 3")
    func defaultRetries() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: []
        )
        #expect(job.retries == 3)
    }

    @Test("CloneJob retries can be overridden")
    func customRetries() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: [],
            retries: 5
        )
        #expect(job.retries == 5)
    }

    @Test("CloneJob skip-preflight defaults to false")
    func skipPreflightDefault() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: []
        )
        #expect(job.skipPreflight == false)
    }

    @Test("CloneStep enableRLS carries SQL")
    func enableRLSStep() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let step = CloneStep.enableRLS(
            sql: "ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;",
            id: id
        )
        if case .enableRLS(let sql, _) = step {
            #expect(sql.contains("ENABLE ROW LEVEL SECURITY"))
        } else {
            Issue.record("Expected enableRLS step")
        }
    }

    @Test("CloneStep attachPartition carries SQL")
    func attachPartitionStep() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders_2024")
        let step = CloneStep.attachPartition(
            sql: "ALTER TABLE public.orders ATTACH PARTITION public.orders_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
            id: id
        )
        if case .attachPartition(let sql, _) = step {
            #expect(sql.contains("ATTACH PARTITION"))
        } else {
            Issue.record("Expected attachPartition step")
        }
    }
}
