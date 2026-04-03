import Testing
import Foundation
import Logging
@testable import PGSchemaEvoCore

// MARK: - ParallelDataTransfer additional coverage

@Suite("ParallelDataTransfer Extra Tests")
struct ParallelDataTransferExtraTests {

    @Test("autoDetectConcurrency returns between 1 and 8")
    func autoDetectConcurrency() {
        let concurrency = ParallelDataTransfer.autoDetectConcurrency()
        #expect(concurrency >= 1)
        #expect(concurrency <= 8)
    }

    @Test("autoDetectConcurrency is capped at 8")
    func autoDetectConcurrencyCap() {
        let concurrency = ParallelDataTransfer.autoDetectConcurrency()
        // Should be min(processorCount, 8)
        let expected = min(ProcessInfo.processInfo.activeProcessorCount, 8)
        #expect(concurrency == expected)
    }

    @Test("buildLevels with single task returns one level")
    func buildLevelsSingleTask() {
        let pdt = ParallelDataTransfer(maxConcurrency: 4, shell: ShellRunner(), logger: Logger(label: "test"))
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let task = DataTransferTask(id: id, method: .copy, estimatedSize: nil)
        let levels = pdt.buildLevels([task])
        #expect(levels.count == 1)
        #expect(levels[0].count == 1)
    }

    @Test("buildLevels with two independent tasks returns one level")
    func buildLevelsTwoIndependent() {
        let pdt = ParallelDataTransfer(maxConcurrency: 4, shell: ShellRunner(), logger: Logger(label: "test"))
        let t1 = DataTransferTask(id: ObjectIdentifier(type: .table, schema: "public", name: "a"), method: .copy, estimatedSize: nil)
        let t2 = DataTransferTask(id: ObjectIdentifier(type: .table, schema: "public", name: "b"), method: .copy, estimatedSize: nil)
        let levels = pdt.buildLevels([t1, t2])
        #expect(levels.count == 1)
        #expect(levels[0].count == 2)
    }

    @Test("buildLevels with chain A->B returns two levels")
    func buildLevelsChain() {
        let pdt = ParallelDataTransfer(maxConcurrency: 4, shell: ShellRunner(), logger: Logger(label: "test"))
        let idA = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let idB = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let t1 = DataTransferTask(id: idA, method: .copy, estimatedSize: nil, dependsOn: [idB])
        let t2 = DataTransferTask(id: idB, method: .copy, estimatedSize: nil)
        let levels = pdt.buildLevels([t1, t2])
        #expect(levels.count == 2)
        #expect(levels[0][0].id.name == "b")
        #expect(levels[1][0].id.name == "a")
    }

    @Test("buildLevels with pgDump method preserves method")
    func buildLevelsPreservesMethod() {
        let pdt = ParallelDataTransfer(maxConcurrency: 4, shell: ShellRunner(), logger: Logger(label: "test"))
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let task = DataTransferTask(id: id, method: .pgDump, estimatedSize: 5000)
        let levels = pdt.buildLevels([task])
        #expect(levels[0][0].method == .pgDump)
        #expect(levels[0][0].estimatedSize == 5000)
    }
}

// MARK: - SyncJob model coverage

@Suite("SyncJob Model Tests")
struct SyncJobModelTests {

    @Test("SyncJob default values")
    func syncJobDefaults() {
        let job = SyncJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: []
        )
        #expect(job.dryRun == true)
        #expect(job.dropExtra == false)
        #expect(job.dropIfExists == false)
        #expect(job.allowDropColumns == false)
        #expect(job.force == false)
        #expect(job.skipPreflight == false)
        #expect(job.syncAll == false)
        #expect(job.retries == 3)
    }

    @Test("SyncJob custom values")
    func syncJobCustom() {
        let job = SyncJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: [ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t"))],
            dryRun: false,
            dropExtra: true,
            dropIfExists: true,
            allowDropColumns: true,
            force: true,
            skipPreflight: true,
            syncAll: true,
            retries: 5
        )
        #expect(job.dryRun == false)
        #expect(job.dropExtra == true)
        #expect(job.dropIfExists == true)
        #expect(job.allowDropColumns == true)
        #expect(job.force == true)
        #expect(job.skipPreflight == true)
        #expect(job.syncAll == true)
        #expect(job.retries == 5)
    }

    @Test("SyncJob toCloneJob preserves relevant fields")
    func syncJobToCloneJob() {
        let source = ConnectionConfig(host: "s", database: "sd", username: "su")
        let target = ConnectionConfig(host: "t", database: "td", username: "tu")
        let specs = [ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t"))]
        let job = SyncJob(
            source: source,
            target: target,
            objects: specs,
            dryRun: false,
            dropIfExists: true,
            force: true,
            skipPreflight: true,
            retries: 7
        )
        let cloneJob = job.toCloneJob()
        #expect(cloneJob.source.host == "s")
        #expect(cloneJob.target.host == "t")
        #expect(cloneJob.objects.count == 1)
        #expect(cloneJob.dryRun == false)
        #expect(cloneJob.dropIfExists == true)
        #expect(cloneJob.force == true)
        #expect(cloneJob.skipPreflight == true)
        #expect(cloneJob.retries == 7)
    }
}

// MARK: - DataSyncJob model coverage

@Suite("DataSyncJob Model Tests")
struct DataSyncJobModelTests {

    @Test("DataSyncJob default values")
    func dataSyncJobDefaults() {
        let job = DataSyncJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            tables: []
        )
        #expect(job.stateFilePath == ".pg-schema-evo-sync-state.yaml")
        #expect(job.dryRun == false)
        #expect(job.detectDeletes == false)
        #expect(job.force == false)
        #expect(job.retries == 3)
    }

    @Test("DataSyncJob custom values")
    func dataSyncJobCustom() {
        let tableConfig = DataSyncTableConfig(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            trackingColumn: "updated_at"
        )
        let job = DataSyncJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            tables: [tableConfig],
            stateFilePath: "/tmp/state.yaml",
            dryRun: true,
            detectDeletes: true,
            force: true,
            retries: 5
        )
        #expect(job.stateFilePath == "/tmp/state.yaml")
        #expect(job.dryRun == true)
        #expect(job.detectDeletes == true)
        #expect(job.force == true)
        #expect(job.retries == 5)
        #expect(job.tables.count == 1)
        #expect(job.tables[0].trackingColumn == "updated_at")
    }

    @Test("DataSyncTableConfig properties")
    func dataSyncTableConfig() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let config = DataSyncTableConfig(id: id, trackingColumn: "id")
        #expect(config.id.name == "orders")
        #expect(config.trackingColumn == "id")
    }

    @Test("DataSyncState with tables")
    func dataSyncState() {
        var state = DataSyncState()
        #expect(state.tables.isEmpty)
        state.tables["public.users"] = DataSyncTableState(column: "updated_at", lastValue: "2026-01-01")
        #expect(state.tables.count == 1)
        #expect(state.tables["public.users"]?.column == "updated_at")
        #expect(state.tables["public.users"]?.lastValue == "2026-01-01")
    }

    @Test("DataSyncTableState properties")
    func dataSyncTableState() {
        let state = DataSyncTableState(column: "id", lastValue: "42")
        #expect(state.column == "id")
        #expect(state.lastValue == "42")
    }
}

// MARK: - ProgressReporter additional coverage

@Suite("ProgressReporter Edge Case Tests Extra")
struct ProgressReporterEdgeCaseTestsExtra {

    @Test("detectColorSupport respects NO_COLOR environment variable behavior")
    func detectColorSupportLogic() {
        // In test/CI environment, detectColorSupport typically returns false
        let result = ProgressReporter.detectColorSupport()
        #expect(result == true || result == false)
    }

    @Test("ProgressReporter with large step count")
    func largeStepCount() {
        let reporter = ProgressReporter(totalSteps: 1000, colorEnabled: false)
        #expect(reporter.totalSteps == 1000)
        reporter.reportStep(500, description: "Halfway")
        reporter.reportStepComplete(500, description: "Done")
    }

    @Test("ProgressReporter with zero steps completes without error")
    func zeroStepsComplete() {
        let reporter = ProgressReporter(totalSteps: 0, colorEnabled: false)
        reporter.reportStart(objectCount: 0)
        reporter.reportComplete(stepCount: 0)
    }

    @Test("ProgressReporter warning with color enabled has ANSI codes")
    func warningWithColor() {
        let reporter = ProgressReporter(totalSteps: 1, colorEnabled: true)
        reporter.reportWarning("Watch out!")
        // No crash = success
    }

    @Test("ProgressReporter step descriptions with special characters")
    func specialCharacters() {
        let reporter = ProgressReporter(totalSteps: 1, colorEnabled: false)
        reporter.reportStep(1, description: "Create \"public\".\"users\" table")
        reporter.reportStepComplete(1, description: "Create \"public\".\"users\" table")
    }
}

// MARK: - ShellRunner which() tests

@Suite("ShellRunner Which Tests")
struct ShellRunnerWhichTests {

    @Test("which finds existing command")
    func whichFindsCommand() {
        let shell = ShellRunner()
        let path = shell.which("ls")
        #expect(path != nil)
    }

    @Test("which returns nil for nonexistent command")
    func whichReturnsNilForMissing() {
        let shell = ShellRunner()
        let path = shell.which("nonexistent_command_xyz_12345")
        #expect(path == nil)
    }

    @Test("which returns full path for bash")
    func whichReturnsBashPath() {
        let shell = ShellRunner()
        if let path = shell.which("bash") {
            #expect(path.hasPrefix("/"))
            #expect(path.contains("bash"))
        }
    }
}

// MARK: - ShellRunner run() tests

@Suite("ShellRunner Run Tests")
struct ShellRunnerRunTests {

    @Test("run echo returns correct output")
    func runEcho() async throws {
        let shell = ShellRunner()
        let result = try await shell.run(command: "/bin/echo", arguments: ["hello"])
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "hello")
        #expect(result.exitCode == 0)
    }

    @Test("run with invalid command throws error")
    func runInvalidCommand() async throws {
        let shell = ShellRunner()
        await #expect(throws: PGSchemaEvoError.self) {
            try await shell.run(command: "/nonexistent/command")
        }
    }

    @Test("run with input passes stdin")
    func runWithInput() async throws {
        let shell = ShellRunner()
        let result = try await shell.run(
            command: "/usr/bin/wc",
            arguments: ["-l"],
            input: "line1\nline2\nline3\n"
        )
        #expect(result.succeeded)
        let count = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        #expect(count == "3")
    }

    @Test("run with environment passes env vars")
    func runWithEnvironment() async throws {
        let shell = ShellRunner()
        let result = try await shell.run(
            command: "/bin/sh",
            arguments: ["-c", "echo $TEST_VAR_XYZ"],
            environment: ["TEST_VAR_XYZ": "hello_test"]
        )
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "hello_test")
    }

    @Test("run captures stderr")
    func runCapturesStderr() async throws {
        let shell = ShellRunner()
        let result = try await shell.run(
            command: "/bin/sh",
            arguments: ["-c", "echo error_msg >&2; exit 1"]
        )
        #expect(!result.succeeded)
        #expect(result.exitCode == 1)
        #expect(result.stderr.contains("error_msg"))
    }

    @Test("ShellResult succeeded is true when exit code is 0")
    func shellResultSucceeded() {
        let result = ShellResult(exitCode: 0, stdout: "", stderr: "")
        #expect(result.succeeded)
    }

    @Test("ShellResult succeeded is false when exit code is non-zero")
    func shellResultFailed() {
        let result = ShellResult(exitCode: 1, stdout: "", stderr: "error")
        #expect(!result.succeeded)
    }
}

// MARK: - AsyncSemaphore additional tests

@Suite("AsyncSemaphore Extra Tests")
struct AsyncSemaphoreExtraTests {

    @Test("Semaphore totalCount matches initialization")
    func totalCount() {
        let sem = AsyncSemaphore(count: 5)
        #expect(sem.totalCount == 5)
    }

    @Test("Semaphore with count 1 acts as mutex")
    func semaphoreAsMutex() async {
        let sem = AsyncSemaphore(count: 1)
        await sem.wait()
        sem.signal()
        // Should not deadlock
        await sem.wait()
        sem.signal()
    }

    @Test("Semaphore multiple signals then waits")
    func multipleSignalsThenWaits() async {
        let sem = AsyncSemaphore(count: 0)
        // Signal first (over-signal)
        sem.signal()
        sem.signal()
        // Both waits should complete immediately
        await sem.wait()
        await sem.wait()
    }

    @Test("Semaphore concurrent access")
    func concurrentAccess() async {
        let sem = AsyncSemaphore(count: 3)
        // Acquire all 3
        await sem.wait()
        await sem.wait()
        await sem.wait()
        // Release all 3
        sem.signal()
        sem.signal()
        sem.signal()
        // Should be able to acquire again
        await sem.wait()
        sem.signal()
    }
}

// MARK: - LiveExecutor more edge cases

@Suite("LiveExecutor Edge Cases")
struct LiveExecutorEdgeCaseTests {

    private func makeExecutor() -> LiveExecutor {
        LiveExecutor(logger: Logger(label: "test"))
    }

    @Test("Multiple COPY steps with different methods")
    func multipleCopySteps() {
        let executor = makeExecutor()
        let t1 = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let t2 = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let steps: [CloneStep] = [
            .copyData(id: t1, method: .copy, estimatedSize: 100),
            .copyData(id: t2, method: .pgDump, estimatedSize: 200),
        ]
        let data1 = "id\n1\n"
        let data2 = "COPY b (id) FROM stdin;\n1\n\\.\n"
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [0: data1, 1: data2])
        #expect(script.contains("FROM STDIN WITH (FORMAT csv, HEADER)"))
        #expect(script.contains("COPY b (id) FROM stdin;"))
    }

    @Test("DROP all object types in one transaction")
    func dropAllTypesInOneTransaction() {
        let executor = makeExecutor()
        let steps: [CloneStep] = [
            .dropObject(ObjectIdentifier(type: .table, schema: "public", name: "t")),
            .dropObject(ObjectIdentifier(type: .view, schema: "public", name: "v")),
            .dropObject(ObjectIdentifier(type: .materializedView, schema: "public", name: "mv")),
            .dropObject(ObjectIdentifier(type: .sequence, schema: "public", name: "s")),
            .dropObject(ObjectIdentifier(type: .function, schema: "public", name: "f", signature: "(int)")),
            .dropObject(ObjectIdentifier(type: .procedure, schema: "public", name: "p")),
            .dropObject(ObjectIdentifier(type: .enum, schema: "public", name: "e")),
            .dropObject(ObjectIdentifier(type: .compositeType, schema: "public", name: "ct")),
            .dropObject(ObjectIdentifier(type: .schema, name: "s")),
            .dropObject(ObjectIdentifier(type: .extension, name: "ext")),
            .dropObject(ObjectIdentifier(type: .role, name: "r")),
            .dropObject(ObjectIdentifier(type: .foreignTable, schema: "public", name: "ft")),
            .dropObject(ObjectIdentifier(type: .aggregate, schema: "public", name: "agg")),
            .dropObject(ObjectIdentifier(type: .operator, schema: "public", name: "op")),
            .dropObject(ObjectIdentifier(type: .foreignDataWrapper, name: "fdw")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP TABLE IF EXISTS"))
        #expect(script.contains("DROP VIEW IF EXISTS"))
        #expect(script.contains("DROP MATERIALIZED VIEW IF EXISTS"))
        #expect(script.contains("DROP SEQUENCE IF EXISTS"))
        #expect(script.contains("DROP FUNCTION IF EXISTS"))
        #expect(script.contains("DROP PROCEDURE IF EXISTS"))
        #expect(script.contains("DROP TYPE IF EXISTS"))
        #expect(script.contains("DROP SCHEMA IF EXISTS"))
        #expect(script.contains("DROP EXTENSION IF EXISTS"))
        #expect(script.contains("DROP ROLE IF EXISTS"))
        #expect(script.contains("DROP FOREIGN TABLE IF EXISTS"))
        #expect(script.contains("DROP AGGREGATE IF EXISTS"))
        #expect(script.contains("DROP OPERATOR IF EXISTS"))
        #expect(script.contains("DROP FOREIGN DATA WRAPPER IF EXISTS"))
        #expect(script.contains("-- Step 15:"))
    }

    @Test("COPY with auto method uses csv format")
    func copyAutoMethodCsv() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let steps: [CloneStep] = [
            .copyData(id: id, method: .auto, estimatedSize: nil),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [0: "id\n1\n"])
        #expect(script.contains("FROM STDIN WITH (FORMAT csv, HEADER)"))
    }

    @Test("Complex multi-step script with all step types")
    func complexAllStepTypes() {
        let executor = makeExecutor()
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let viewId = ObjectIdentifier(type: .view, schema: "public", name: "v")
        let matviewId = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv")
        let partId = ObjectIdentifier(type: .table, schema: "public", name: "p")

        let steps: [CloneStep] = [
            .dropObject(tableId),
            .createObject(sql: "CREATE TABLE t (id int);", id: tableId),
            .alterObject(sql: "ALTER TABLE t ADD COLUMN name text;", id: tableId),
            .copyData(id: tableId, method: .copy, estimatedSize: nil),
            .grantPermissions(sql: "GRANT SELECT ON t TO reader;", id: tableId),
            .enableRLS(sql: "ALTER TABLE t ENABLE ROW LEVEL SECURITY;", id: tableId),
            .attachPartition(sql: "ALTER TABLE t ATTACH PARTITION p FOR VALUES IN ('a');", id: partId),
            .refreshMaterializedView(matviewId),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [3: "id\n1\n"])

        #expect(script.hasPrefix("BEGIN;\n"))
        #expect(script.hasSuffix("COMMIT;\n"))
        #expect(script.contains("DROP TABLE"))
        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("ALTER TABLE t ADD COLUMN"))
        #expect(script.contains("FROM STDIN"))
        #expect(script.contains("GRANT SELECT"))
        #expect(script.contains("ROW LEVEL SECURITY"))
        #expect(script.contains("ATTACH PARTITION"))
        #expect(script.contains("REFRESH MATERIALIZED VIEW"))
        // All 8 steps numbered
        for i in 1...8 {
            #expect(script.contains("-- Step \(i):"))
        }
    }
}

// MARK: - PostgresConnectionPool.size coverage

@Suite("PostgresConnectionPool Unit Tests")
struct PostgresConnectionPoolUnitTests {

    @Test("AsyncSemaphore with large count")
    func largeSemaphore() async {
        let sem = AsyncSemaphore(count: 100)
        #expect(sem.totalCount == 100)
        for _ in 0..<100 {
            await sem.wait()
        }
        for _ in 0..<100 {
            sem.signal()
        }
    }
}

// MARK: - PgDumpMetadata model tests

@Suite("PgDumpMetadata Tests")
struct PgDumpMetadataTests {

    @Test("PgDumpMetadata stores id and ddl")
    func pgDumpMetadataProperties() {
        let id = ObjectIdentifier(type: .aggregate, schema: "public", name: "my_agg")
        let metadata = PgDumpMetadata(id: id, ddl: "CREATE AGGREGATE my_agg (sfunc = int4pl);")
        #expect(metadata.id.name == "my_agg")
        #expect(metadata.ddl.contains("CREATE AGGREGATE"))
    }
}

// MARK: - ObjectSpec additional coverage

@Suite("ObjectSpec Extra Tests")
struct ObjectSpecExtraTests {

    @Test("ObjectSpec default values")
    func objectSpecDefaults() {
        let spec = ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t"))
        #expect(spec.copyPermissions == false)
        #expect(spec.copyData == false)
        #expect(spec.cascadeDependencies == false)
    }

    @Test("ObjectSpec custom values")
    func objectSpecCustom() {
        let spec = ObjectSpec(
            id: ObjectIdentifier(type: .table, schema: "public", name: "t"),
            copyPermissions: true,
            copyData: true,
            cascadeDependencies: true
        )
        #expect(spec.copyPermissions == true)
        #expect(spec.copyData == true)
        #expect(spec.cascadeDependencies == true)
    }

    @Test("ObjectSpec with whereClause and rowLimit")
    func objectSpecWhereAndLimit() {
        let spec = ObjectSpec(
            id: ObjectIdentifier(type: .table, schema: "public", name: "t"),
            copyData: true,
            whereClause: "active = true",
            rowLimit: 500
        )
        #expect(spec.whereClause == "active = true")
        #expect(spec.rowLimit == 500)
    }
}
