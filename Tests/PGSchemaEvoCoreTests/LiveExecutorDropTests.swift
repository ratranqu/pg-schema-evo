import Testing
import Logging
@testable import PGSchemaEvoCore

@Suite("LiveExecutor DROP SQL Tests")
struct LiveExecutorDropTests {

    private func makeExecutor() -> LiveExecutor {
        LiveExecutor(logger: Logger(label: "test"))
    }

    // MARK: - DROP SQL for all object types

    @Test("DROP VIEW generates correct SQL")
    func dropView() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP VIEW IF EXISTS \"public\".\"active_users\" CASCADE;"))
    }

    @Test("DROP MATERIALIZED VIEW generates correct SQL")
    func dropMaterializedView() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv_stats")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP MATERIALIZED VIEW IF EXISTS \"public\".\"mv_stats\" CASCADE;"))
    }

    @Test("DROP SEQUENCE generates correct SQL")
    func dropSequence() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "users_id_seq")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP SEQUENCE IF EXISTS \"public\".\"users_id_seq\" CASCADE;"))
    }

    @Test("DROP FUNCTION without signature uses empty parens")
    func dropFunctionNoSignature() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .function, schema: "public", name: "my_func")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP FUNCTION IF EXISTS \"public\".\"my_func\"() CASCADE;"))
    }

    @Test("DROP PROCEDURE with signature")
    func dropProcedureWithSignature() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .procedure, schema: "public", name: "cleanup", signature: "(integer, text)")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP PROCEDURE IF EXISTS \"public\".\"cleanup\"(integer, text) CASCADE;"))
    }

    @Test("DROP PROCEDURE without signature uses empty parens")
    func dropProcedureNoSignature() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .procedure, schema: "public", name: "do_work")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP PROCEDURE IF EXISTS \"public\".\"do_work\"() CASCADE;"))
    }

    @Test("DROP TYPE for enum generates correct SQL")
    func dropEnum() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "status_type")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP TYPE IF EXISTS \"public\".\"status_type\" CASCADE;"))
    }

    @Test("DROP TYPE for composite type generates correct SQL")
    func dropCompositeType() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP TYPE IF EXISTS \"public\".\"address\" CASCADE;"))
    }

    @Test("DROP SCHEMA generates correct SQL")
    func dropSchema() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .schema, name: "analytics")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP SCHEMA IF EXISTS \"analytics\" CASCADE;"))
    }

    @Test("DROP EXTENSION generates correct SQL")
    func dropExtension() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .extension, name: "uuid-ossp")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP EXTENSION IF EXISTS \"uuid-ossp\" CASCADE;"))
    }

    @Test("DROP ROLE generates correct SQL")
    func dropRole() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .role, name: "readonly")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP ROLE IF EXISTS \"readonly\";"))
    }

    @Test("DROP FOREIGN TABLE generates correct SQL")
    func dropForeignTable() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .foreignTable, schema: "public", name: "remote_data")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP FOREIGN TABLE IF EXISTS \"public\".\"remote_data\" CASCADE;"))
    }

    @Test("DROP AGGREGATE with signature")
    func dropAggregate() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .aggregate, schema: "public", name: "array_agg_custom", signature: "(integer)")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP AGGREGATE IF EXISTS \"public\".\"array_agg_custom\"(integer) CASCADE;"))
    }

    @Test("DROP OPERATOR generates correct SQL")
    func dropOperator() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .operator, schema: "public", name: "===")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP OPERATOR IF EXISTS \"public\".\"===\" CASCADE;"))
    }

    @Test("DROP FOREIGN DATA WRAPPER generates correct SQL")
    func dropForeignDataWrapper() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .foreignDataWrapper, name: "postgres_fdw")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP FOREIGN DATA WRAPPER IF EXISTS \"postgres_fdw\" CASCADE;"))
    }

    // MARK: - Step descriptions in comments

    @Test("Step description for copyData includes method")
    func copyDataStepDescription() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .pgDump, estimatedSize: 1000),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("Copy data"))
        #expect(script.contains("pgdump"))
    }

    @Test("Step description for grantPermissions")
    func grantPermissionsStepDescription() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let steps: [CloneStep] = [
            .grantPermissions(sql: "GRANT SELECT ON t TO reader;", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("Grant permissions"))
    }

    @Test("Step description for refreshMaterializedView")
    func refreshMatviewStepDescription() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv")
        let steps: [CloneStep] = [.refreshMaterializedView(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("Refresh materialized view"))
    }

    @Test("Step description for enableRLS")
    func enableRLSStepDescription() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let steps: [CloneStep] = [
            .enableRLS(sql: "ALTER TABLE t ENABLE ROW LEVEL SECURITY;", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("Enable RLS"))
    }

    @Test("Step description for attachPartition")
    func attachPartitionStepDescription() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "p1")
        let steps: [CloneStep] = [
            .attachPartition(sql: "ALTER TABLE orders ATTACH PARTITION p1;", id: id),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("Attach partition"))
    }

    // MARK: - Complex scripts

    @Test("Complex script with DROP, CREATE, COPY, GRANT, RLS, PARTITION")
    func complexScript() {
        let executor = makeExecutor()
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let partId = ObjectIdentifier(type: .table, schema: "public", name: "users_2024")

        let steps: [CloneStep] = [
            .dropObject(tableId),
            .createObject(sql: "CREATE TABLE users (id int);", id: tableId),
            .copyData(id: tableId, method: .copy, estimatedSize: 100),
            .grantPermissions(sql: "GRANT SELECT ON users TO reader;", id: tableId),
            .enableRLS(sql: "ALTER TABLE users ENABLE ROW LEVEL SECURITY;", id: tableId),
            .attachPartition(sql: "ALTER TABLE users ATTACH PARTITION users_2024;", id: partId),
        ]
        let csvData = "id\n1\n2\n"
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [2: csvData])

        #expect(script.hasPrefix("BEGIN;\n"))
        #expect(script.hasSuffix("COMMIT;\n"))
        // Check all 6 steps are numbered
        #expect(script.contains("-- Step 1:"))
        #expect(script.contains("-- Step 2:"))
        #expect(script.contains("-- Step 3:"))
        #expect(script.contains("-- Step 4:"))
        #expect(script.contains("-- Step 5:"))
        #expect(script.contains("-- Step 6:"))
    }
}
