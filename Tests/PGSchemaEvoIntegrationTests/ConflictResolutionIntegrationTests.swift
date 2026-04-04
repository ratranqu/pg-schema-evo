import Testing
import PostgresNIO
import Logging
import Foundation
@testable import PGSchemaEvoCore

@Suite("Conflict Resolution Integration Tests", .tags(.integration), .serialized)
struct ConflictResolutionIntegrationTests {

    private static let testSchema = "_conflict_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    private static func cleanup(sourceConn: PostgresConnection, targetConn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).conflict_table CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).conflict_table CASCADE", on: targetConn)
    }

    // MARK: - Fail strategy halts on conflict

    @Test("Sync with fail strategy throws on conflict")
    func failStrategyThrows() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await Self.cleanup(sourceConn: sourceConn, targetConn: targetConn)

        // Source: table with 2 columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target: same table with an extra column (conflict)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_table")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true,
            conflictStrategy: .fail
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)

        // Should throw because there's a conflict and strategy is .fail
        do {
            _ = try await orchestrator.execute(job: syncJob)
            #expect(Bool(false), "Expected conflictsDetected error")
        } catch let error as PGSchemaEvoError {
            if case .conflictsDetected(let count, let destructive) = error {
                #expect(count > 0)
                #expect(destructive > 0)
            } else {
                #expect(Bool(false), "Unexpected error type: \(error)")
            }
        }

        // Clean up
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await Self.cleanup(sourceConn: sc, targetConn: tc)
        try? await sc.close()
        try? await tc.close()
    }

    // MARK: - Source wins with force drops extra column

    @Test("Sync with source-wins and force drops extra column")
    func sourceWinsForceDropsExtra() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await Self.cleanup(sourceConn: sourceConn, targetConn: targetConn)

        // Source: 2 columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target: 3 columns (extra one)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_table")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true,
            conflictStrategy: .sourceWins
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        // Verify extra column was dropped
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'conflict_table' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(!columns.contains("extra_col"))
        #expect(columns.contains("id"))
        #expect(columns.contains("name"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Target wins keeps extra column

    @Test("Sync with target-wins keeps extra column")
    func targetWinsKeepsExtra() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await Self.cleanup(sourceConn: sourceConn, targetConn: targetConn)

        // Source: 2 columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target: 3 columns (extra one)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_table")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true,
            conflictStrategy: .targetWins
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        // Verify extra column is preserved
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'conflict_table' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(columns.contains("extra_col"))
        #expect(columns.contains("id"))
        #expect(columns.contains("name"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Source wins without force blocks destructive

    @Test("Sync with source-wins without force blocks destructive")
    func sourceWinsWithoutForceBlocks() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await Self.cleanup(sourceConn: sourceConn, targetConn: targetConn)

        // Source: 2 columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target: extra column (destructive to drop)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_table")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: false,  // No force — destructive should be blocked
            conflictStrategy: .sourceWins
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)

        do {
            _ = try await orchestrator.execute(job: syncJob)
            #expect(Bool(false), "Expected destructiveActionBlocked error")
        } catch let error as PGSchemaEvoError {
            if case .destructiveActionBlocked = error {
                // Expected
            } else {
                #expect(Bool(false), "Unexpected error type: \(error)")
            }
        }

        // Clean up
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await Self.cleanup(sourceConn: sc, targetConn: tc)
        try? await sc.close()
        try? await tc.close()
    }

    // MARK: - Conflict file round-trip

    @Test("Conflict file generation and re-application")
    func conflictFileRoundTrip() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await Self.cleanup(sourceConn: sourceConn, targetConn: targetConn)

        // Source: 2 columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target: extra column
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let conflictFilePath = NSTemporaryDirectory() + "conflict-integration-\(UUID().uuidString).json"
        defer { try? FileManager.default.removeItem(atPath: conflictFilePath) }

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_table")

        // Step 1: Generate conflict file
        let syncJob1 = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true,
            conflictStrategy: .fail,
            conflictFilePath: conflictFilePath
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let output = try await orchestrator.execute(job: syncJob1)
        #expect(output.contains("Conflict report written"))

        // Verify file was created
        #expect(FileManager.default.fileExists(atPath: conflictFilePath))

        // Step 2: Edit the file to resolve all as apply-source
        var data = try Data(contentsOf: URL(fileURLWithPath: conflictFilePath))
        var json = String(data: data, encoding: .utf8)!
        json = json.replacingOccurrences(of: "\"resolution\" : null", with: "\"resolution\" : \"apply-source\"")
        data = json.data(using: .utf8)!
        try data.write(to: URL(fileURLWithPath: conflictFilePath))

        // Step 3: Apply resolutions from file
        let syncJob2 = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true,
            conflictStrategy: .sourceWins,
            resolveFromPath: conflictFilePath
        )
        _ = try await orchestrator.execute(job: syncJob2)

        // Verify extra column was dropped
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'conflict_table' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(!columns.contains("extra_col"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Skip strategy skips conflicts

    @Test("Sync with skip strategy skips conflicting objects")
    func skipStrategySkips() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await Self.cleanup(sourceConn: sourceConn, targetConn: targetConn)

        // Source: 2 columns
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL
            )
        """, on: sourceConn)

        // Target: extra column
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).conflict_table (
                id integer PRIMARY KEY,
                name text NOT NULL,
                extra_col text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "conflict_table")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true,
            conflictStrategy: .skip
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        // Verify extra column is still present (conflict was skipped)
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT column_name FROM information_schema.columns WHERE table_schema = '\(Self.testSchema)' AND table_name = 'conflict_table' ORDER BY ordinal_position"),
            logger: IntegrationTestConfig.logger
        )
        var columns: [String] = []
        for try await row in rows {
            columns.append(try row.decode(String.self))
        }
        #expect(columns.contains("extra_col"))

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).conflict_table CASCADE", on: sc)
        try? await sc.close()
    }
}
