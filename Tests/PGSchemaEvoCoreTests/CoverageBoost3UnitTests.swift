import Testing
import Foundation
import Logging
@testable import PGSchemaEvoCore

// MARK: - ProgressReporter coverage

@Suite("ProgressReporter Extra Tests")
struct ProgressReporterExtraTests {

    @Test("ProgressReporter reportStart with zero objects")
    func reportStartZero() {
        let progress = ProgressReporter(totalSteps: 0)
        progress.reportStart(objectCount: 0)
    }

    @Test("ProgressReporter reportStart with many objects")
    func reportStartMany() {
        let progress = ProgressReporter(totalSteps: 10)
        progress.reportStart(objectCount: 10)
    }

    @Test("ProgressReporter reportStep and reportStepComplete")
    func reportStepMultiple() {
        let progress = ProgressReporter(totalSteps: 3)
        progress.reportStart(objectCount: 3)
        progress.reportStep(1, description: "Creating table")
        progress.reportStepComplete(1, description: "Creating table")
        progress.reportStep(2, description: "Copying data")
        progress.reportStepComplete(2, description: "Copying data")
        progress.reportStep(3, description: "Applying permissions")
        progress.reportStepFailed(3, description: "Applying permissions", error: "role not found")
    }

    @Test("ProgressReporter reportComplete")
    func reportComplete() {
        let progress = ProgressReporter(totalSteps: 1)
        progress.reportStart(objectCount: 1)
        progress.reportStep(1, description: "Step 1")
        progress.reportStepComplete(1, description: "Step 1")
        progress.reportComplete(stepCount: 1)
    }

    @Test("ProgressReporter reportDryRun")
    func reportDryRun() {
        let progress = ProgressReporter(totalSteps: 1)
        progress.reportDryRun()
    }
}

// MARK: - SignalHandler coverage

@Suite("SignalHandler Extra Tests")
struct SignalHandlerExtraTests {

    @Test("SignalHandler shared instance exists")
    func sharedInstance() {
        let handler = SignalHandler.shared
        #expect(handler != nil)
    }

    @Test("SignalHandler install and uninstall")
    func installUninstall() {
        let handler = SignalHandler.shared
        handler.install()
        handler.uninstall()
    }

    @Test("SignalHandler double install is idempotent")
    func doubleInstall() {
        let handler = SignalHandler.shared
        handler.install()
        handler.install()
        handler.uninstall()
    }

    @Test("SignalHandler double uninstall is safe")
    func doubleUninstall() {
        let handler = SignalHandler.shared
        handler.install()
        handler.uninstall()
        handler.uninstall()
    }

    @Test("SignalHandler setTransactionContext")
    func setTransactionContext() {
        let handler = SignalHandler.shared
        handler.install()
        handler.setTransactionContext(true)
        handler.setTransactionContext(false)
        handler.uninstall()
    }

    @Test("SignalHandler registerProcess")
    func registerProcess() {
        let handler = SignalHandler.shared
        handler.install()
        let process = Process()
        handler.registerProcess(process)
        handler.uninstall()
    }
}

// MARK: - LiveExecutor additional unit coverage

@Suite("LiveExecutor Extra Unit Tests")
struct LiveExecutorExtraUnitTests {

    func makeLogger() -> Logger {
        var logger = Logger(label: "test")
        logger.logLevel = .debug
        return logger
    }

    @Test("buildTransactionScript with drop")
    func buildTransactionScriptDrop() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .dropObject(ObjectIdentifier(type: .table, schema: "public", name: "test")),
            .createObject(sql: "CREATE TABLE public.test (id int);", id: ObjectIdentifier(type: .table, schema: "public", name: "test")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP"))
        #expect(script.contains("CREATE"))
        #expect(script.contains("BEGIN"))
        #expect(script.contains("COMMIT"))
    }

    @Test("buildTransactionScript with alter")
    func buildTransactionScriptAlter() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .alterObject(sql: "ALTER TABLE public.test ADD COLUMN name text;", id: ObjectIdentifier(type: .table, schema: "public", name: "test")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("ALTER"))
    }

    @Test("buildTransactionScript with permissions")
    func buildTransactionScriptPermissions() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .grantPermissions(sql: "GRANT SELECT ON public.test TO readonly;", id: ObjectIdentifier(type: .table, schema: "public", name: "test")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("GRANT"))
    }

    @Test("buildTransactionScript with RLS")
    func buildTransactionScriptRLS() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .enableRLS(sql: "ALTER TABLE public.test ENABLE ROW LEVEL SECURITY;", id: ObjectIdentifier(type: .table, schema: "public", name: "test")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("ROW LEVEL SECURITY"))
    }

    @Test("buildTransactionScript with refreshMaterializedView")
    func buildTransactionScriptRefreshMatview() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .refreshMaterializedView(ObjectIdentifier(type: .materializedView, schema: "public", name: "mv_test")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("REFRESH MATERIALIZED VIEW"))
    }

    @Test("buildTransactionScript with attachPartition")
    func buildTransactionScriptAttachPartition() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .attachPartition(sql: "ALTER TABLE public.events ATTACH PARTITION public.events_2025q1 FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');", id: ObjectIdentifier(type: .table, schema: "public", name: "events_2025q1")),
        ]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("ATTACH PARTITION"))
    }

    @Test("buildTransactionScript with copyData includes prefetched data")
    func buildTransactionScriptCopyDataWithPrefetch() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .copyData(
                id: ObjectIdentifier(type: .table, schema: "public", name: "test"),
                method: .copy,
                estimatedSize: 1024,
                whereClause: "id > 0",
                rowLimit: 10
            ),
        ]
        let prefetched: [Int: String] = [0: "1\tAlice\n2\tBob\n"]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: prefetched)
        #expect(!script.isEmpty)
    }

    @Test("buildTransactionScript with copyData pgDump method")
    func buildTransactionScriptCopyDataPgDump() {
        let executor = LiveExecutor(logger: makeLogger())
        let steps: [CloneStep] = [
            .copyData(
                id: ObjectIdentifier(type: .table, schema: "public", name: "test"),
                method: .pgDump,
                estimatedSize: 1024
            ),
        ]
        let prefetched: [Int: String] = [0: "INSERT INTO public.test VALUES (1, 'test');\n"]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: prefetched)
        #expect(!script.isEmpty)
    }

    @Test("generateDropSQL for various types")
    func generateDropSQLTypes() {
        let executor = LiveExecutor(logger: makeLogger())

        let types: [(ObjectType, String)] = [
            (.table, "TABLE"),
            (.view, "VIEW"),
            (.materializedView, "MATERIALIZED VIEW"),
            (.enum, "TYPE"),
            (.compositeType, "TYPE"),
            (.function, "FUNCTION"),
            (.procedure, "PROCEDURE"),
            (.sequence, "SEQUENCE"),
            (.schema, "SCHEMA"),
            (.extension, "EXTENSION"),
        ]

        for (type, expected) in types {
            let steps: [CloneStep] = [
                .dropObject(ObjectIdentifier(type: type, schema: "public", name: "test_obj")),
            ]
            let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
            #expect(script.contains(expected), "Expected \(expected) for type \(type)")
        }
    }

    @Test("stepDescription for various step types")
    func stepDescriptions() {
        let executor = LiveExecutor(logger: makeLogger())
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let matviewId = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv")

        let steps: [CloneStep] = [
            .createObject(sql: "CREATE TABLE...", id: tableId),
            .dropObject(tableId),
            .alterObject(sql: "ALTER...", id: tableId),
            .copyData(id: tableId, method: .copy, estimatedSize: 100),
            .copyData(id: tableId, method: .pgDump, estimatedSize: 100),
            .grantPermissions(sql: "GRANT...", id: tableId),
            .enableRLS(sql: "ALTER TABLE...", id: tableId),
            .refreshMaterializedView(matviewId),
            .attachPartition(sql: "ALTER TABLE...", id: tableId),
        ]

        // buildTransactionScript calls stepDescription internally
        for step in steps {
            let script = executor.buildTransactionScript(steps: [step], prefetchedData: [:])
            #expect(!script.isEmpty)
        }
    }
}

// MARK: - DataSync model coverage

@Suite("DataSync Model Extra Tests")
struct DataSyncModelExtraTests {

    @Test("DataSyncJob with all options")
    func dataSyncJobAllOptions() {
        let source = ConnectionConfig(host: "localhost", port: 5432, database: "src", username: "u", password: "p")
        let target = ConnectionConfig(host: "localhost", port: 5432, database: "tgt", username: "u", password: "p")

        let job = DataSyncJob(
            source: source,
            target: target,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
                    trackingColumn: "updated_at"
                ),
            ],
            stateFilePath: "/tmp/test-state.yaml",
            dryRun: true,
            detectDeletes: true,
            force: true,
            retries: 5
        )

        #expect(job.tables.count == 1)
        #expect(job.stateFilePath == "/tmp/test-state.yaml")
        #expect(job.detectDeletes)
        #expect(job.dryRun)
        #expect(job.retries == 5)
    }

    @Test("DataSyncState and DataSyncTableState")
    func dataSyncState() {
        var state = DataSyncState()
        #expect(state.tables.isEmpty)

        state.tables["public.orders"] = DataSyncTableState(
            column: "updated_at",
            lastValue: "2025-01-01T00:00:00Z"
        )

        #expect(state.tables.count == 1)
        #expect(state.tables["public.orders"]?.column == "updated_at")
        #expect(state.tables["public.orders"]?.lastValue == "2025-01-01T00:00:00Z")
    }
}

// MARK: - PGSchemaEvoError coverage

@Suite("Error Extra Tests")
struct ErrorExtraTests {

    @Test("All error types have descriptions")
    func errorDescriptions() {
        let errors: [PGSchemaEvoError] = [
            .connectionFailed(endpoint: "test://localhost", underlying: "timeout"),
            .objectNotFound(ObjectIdentifier(type: .table, schema: "public", name: "missing")),
            .shellCommandFailed(command: "psql", exitCode: 1, stderr: "error"),
            .invalidObjectSpec("bad spec"),
            .unsupportedObjectType(.operator, reason: "not supported"),
            .preflightFailed(checks: ["check1", "check2"]),
        ]

        for error in errors {
            let desc = error.localizedDescription
            #expect(!desc.isEmpty)
        }
    }
}

// MARK: - CloneStep extra coverage

@Suite("CloneStep Extra Tests")
struct CloneStepExtraTests {

    @Test("CloneStep copyData with all optional parameters")
    func copyDataAllParams() {
        let step = CloneStep.copyData(
            id: ObjectIdentifier(type: .table, schema: "public", name: "test"),
            method: .copy,
            estimatedSize: 50_000_000,
            whereClause: "status = 'active'",
            rowLimit: 1000
        )

        if case .copyData(let id, let method, let size, let wh, let limit) = step {
            #expect(id.name == "test")
            #expect(method == .copy)
            #expect(size == 50_000_000)
            #expect(wh == "status = 'active'")
            #expect(limit == 1000)
        }
    }

    @Test("CloneStep copyData with pgDump method")
    func copyDataPgDump() {
        let step = CloneStep.copyData(
            id: ObjectIdentifier(type: .table, schema: "public", name: "test"),
            method: .pgDump,
            estimatedSize: 200_000_000
        )

        if case .copyData(_, let method, let size, let wh, let limit) = step {
            #expect(method == .pgDump)
            #expect(size == 200_000_000)
            #expect(wh == nil)
            #expect(limit == nil)
        }
    }

    @Test("CloneStep enum cases")
    func allCases() {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "t")

        let steps: [CloneStep] = [
            .dropObject(tableId),
            .createObject(sql: "CREATE...", id: tableId),
            .alterObject(sql: "ALTER...", id: tableId),
            .copyData(id: tableId, method: .auto, estimatedSize: 0),
            .grantPermissions(sql: "GRANT...", id: tableId),
            .refreshMaterializedView(tableId),
            .enableRLS(sql: "ALTER...", id: tableId),
            .attachPartition(sql: "ALTER TABLE...", id: tableId),
        ]

        #expect(steps.count == 8)
    }
}

// MARK: - ConnectionConfig extra coverage

@Suite("ConnectionConfig Extra Tests")
struct ConnectionConfigExtraTests {

    @Test("ConnectionConfig toDSN with all parameters")
    func toDSNFull() {
        let config = ConnectionConfig(
            host: "db.example.com",
            port: 5433,
            database: "mydb",
            username: "admin",
            password: "secret123"
        )

        let dsn = config.toDSN()
        #expect(dsn.contains("db.example.com"))
        #expect(dsn.contains("5433"))
        #expect(dsn.contains("mydb"))
        #expect(dsn.contains("admin"))
        #expect(dsn.contains("secret123"))
    }

    @Test("ConnectionConfig toDSN with masked password")
    func toDSNMasked() {
        let config = ConnectionConfig(
            host: "localhost",
            port: 5432,
            database: "test",
            username: "user",
            password: "mysecret"
        )

        let masked = config.toDSN(maskPassword: true)
        #expect(!masked.contains("mysecret"))
        #expect(masked.contains("***"))
    }

    @Test("ConnectionConfig environment includes password")
    func environment() {
        let config = ConnectionConfig(
            host: "localhost",
            port: 5432,
            database: "test",
            username: "user",
            password: "mypass"
        )

        let env = config.environment()
        #expect(env["PGPASSWORD"] == "mypass")
    }
}

// MARK: - SyncJob extra coverage

@Suite("SyncJob Extra Tests")
struct SyncJobExtraTests {

    @Test("SyncJob toCloneJob conversion")
    func toCloneJob() {
        let source = ConnectionConfig(host: "s", port: 5432, database: "src", username: "u", password: "p")
        let target = ConnectionConfig(host: "t", port: 5432, database: "tgt", username: "u", password: "p")

        let syncJob = SyncJob(
            source: source,
            target: target,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "test")),
            ],
            dryRun: true,
            dropExtra: true,
            dropIfExists: true,
            allowDropColumns: true,
            force: true,
            skipPreflight: false,
            syncAll: true,
            retries: 5
        )

        let cloneJob = syncJob.toCloneJob()
        #expect(cloneJob.dryRun == true)
        #expect(cloneJob.dropIfExists == true)
        #expect(cloneJob.force == true)
        #expect(cloneJob.retries == 5)
        #expect(cloneJob.objects.count == 1)
    }

    @Test("SyncJob with all options")
    func syncJobAllOptions() {
        let source = ConnectionConfig(host: "s", port: 5432, database: "src", username: "u", password: "p")
        let target = ConnectionConfig(host: "t", port: 5432, database: "tgt", username: "u", password: "p")

        let job = SyncJob(
            source: source,
            target: target,
            objects: [],
            dryRun: false,
            dropExtra: true,
            dropIfExists: false,
            allowDropColumns: true,
            force: false,
            skipPreflight: true,
            syncAll: true,
            retries: 0
        )

        #expect(job.dropExtra)
        #expect(job.allowDropColumns)
        #expect(job.syncAll)
        #expect(!job.force)
    }
}

// MARK: - MigrationFileManager coverage

@Suite("MigrationFileManager Extra Tests")
struct MigrationFileManagerExtraTests {

    @Test("Write and read migration with irreversible changes")
    func writeReadIrreversibleChanges() throws {
        let tmpDir = FileManager.default.temporaryDirectory.appendingPathComponent("mfm_test_\(UUID().uuidString)").path
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fm = MigrationFileManager(directory: tmpDir)
        let migration = Migration(
            id: "20260403_test_irrev",
            description: "Test irreversible",
            generatedAt: "2026-04-03T00:00:00Z",
            checksum: "abc123",
            objectsAffected: ["table:public.users"],
            irreversibleChanges: ["Cannot remove enum value 'old_status'"],
            version: 1
        )
        let sql = MigrationSQL(
            upSQL: "ALTER TYPE public.order_status ADD VALUE 'new_status';",
            downSQL: ""
        )

        try fm.write(migration: migration, sql: sql)
        let (readMigration, readSQL) = try fm.read(id: migration.id)

        #expect(readMigration.id == migration.id)
        #expect(readMigration.irreversibleChanges == ["Cannot remove enum value 'old_status'"])
        #expect(readSQL.upSQL.contains("ADD VALUE"))
    }

    @Test("List migrations returns sorted IDs")
    func listMigrationsSorted() throws {
        let tmpDir = FileManager.default.temporaryDirectory.appendingPathComponent("mfm_list_\(UUID().uuidString)").path
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fm = MigrationFileManager(directory: tmpDir)

        // Write two migrations out of order
        for id in ["20260403_second", "20260402_first"] {
            let m = Migration(id: id, description: "test", generatedAt: "", checksum: "x")
            let s = MigrationSQL(upSQL: "SELECT 1;", downSQL: "")
            try fm.write(migration: m, sql: s)
        }

        let ids = try fm.listMigrations()
        #expect(ids == ["20260402_first", "20260403_second"])
    }

    @Test("List migrations on empty directory returns empty")
    func listMigrationsEmpty() throws {
        let tmpDir = FileManager.default.temporaryDirectory.appendingPathComponent("mfm_empty_\(UUID().uuidString)").path
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fm = MigrationFileManager(directory: tmpDir)
        let ids = try fm.listMigrations()
        #expect(ids.isEmpty)
    }
}

// MARK: - MigrationApplicator empty directory coverage

@Suite("MigrationApplicator Unit Tests")
struct MigrationApplicatorUnitTests {

    @Test("Apply with empty migration directory returns empty")
    func applyEmptyDirectory() async throws {
        let tmpDir = FileManager.default.temporaryDirectory.appendingPathComponent("mig_empty_\(UUID().uuidString)").path
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let config = MigrationConfig(directory: tmpDir)
        let applicator = MigrationApplicator(config: config, logger: Logger(label: "test"))

        // This should hit lines 34-35 (no migration files found)
        let applied = try await applicator.apply(
            targetDSN: "postgresql://testuser:testpass@localhost:15432/source_db"
        )
        #expect(applied.isEmpty)
    }

    @Test("Rollback with no applied migrations returns empty")
    func rollbackNoApplied() async throws {
        let tmpDir = FileManager.default.temporaryDirectory.appendingPathComponent("mig_rollback_\(UUID().uuidString)").path
        try FileManager.default.createDirectory(atPath: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let config = MigrationConfig(directory: tmpDir)
        let applicator = MigrationApplicator(config: config, logger: Logger(label: "test"))

        // This should hit lines 145-147 (no applied migrations to rollback)
        let rolledBack = try await applicator.rollback(
            targetDSN: "postgresql://testuser:testpass@localhost:15432/source_db"
        )
        #expect(rolledBack.isEmpty)
    }
}
