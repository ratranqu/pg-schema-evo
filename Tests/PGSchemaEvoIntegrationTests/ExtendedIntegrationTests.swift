import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Extended Integration Tests", .tags(.integration), .serialized)
struct ExtendedIntegrationTests {

    private static let testSchema = "_ext_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - YAML Config File Loading

    @Test("Config loader rejects missing config file")
    func configLoaderMissingFile() async throws {
        let loader = ConfigLoader()
        #expect(throws: PGSchemaEvoError.self) {
            _ = try loader.load(path: "/nonexistent/path/config.yaml")
        }
    }

    @Test("Config loader with overrides respects CLI flags")
    func configLoaderOverrides() async throws {
        let tmpDir = NSTemporaryDirectory()
        let configPath = tmpDir + "test-override-config-\(UUID().uuidString).yaml"
        let yaml = """
        source:
          dsn: "\(IntegrationTestConfig.sourceDSN)"
        target:
          dsn: "\(IntegrationTestConfig.targetDSN)"
        defaults:
          drop_existing: false
        objects:
          - type: table
            schema: public
            name: users
        """
        try yaml.write(toFile: configPath, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: configPath) }

        let loader = ConfigLoader()
        let overrides = ConfigOverrides(dropExisting: true, force: true)
        let config = try loader.load(path: configPath, overrides: overrides)

        #expect(config.dropIfExists == true, "CLI override should win over config file")
    }

    @Test("Config loader parses YAML and produces valid CloneJobConfig")
    func configLoaderEndToEnd() async throws {
        let tmpDir = NSTemporaryDirectory()
        let configPath = tmpDir + "test-clone-config-\(UUID().uuidString).yaml"
        let yaml = """
        source:
          dsn: "\(IntegrationTestConfig.sourceDSN)"
        target:
          dsn: "\(IntegrationTestConfig.targetDSN)"
        defaults:
          data: true
          permissions: false
          drop_existing: true
        objects:
          - type: table
            schema: public
            name: products
            data: true
          - type: enum
            schema: public
            name: order_status
        """
        try yaml.write(toFile: configPath, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: configPath) }

        let loader = ConfigLoader()
        let config = try loader.load(path: configPath)

        #expect(config.objects.count == 2)
        #expect(config.objects[0].id.type == .table)
        #expect(config.objects[0].id.name == "products")
        #expect(config.objects[0].copyData == true)
        #expect(config.objects[1].id.type == .enum)
        #expect(config.objects[1].id.name == "order_status")
        #expect(config.dropIfExists == true)

        // Verify the config can produce a dry-run clone
        let baseJob = config.toCloneJob()
        let cloneJob = CloneJob(
            source: baseJob.source,
            target: baseJob.target,
            objects: baseJob.objects,
            dryRun: true,
            defaultDataMethod: baseJob.defaultDataMethod,
            dataSizeThreshold: baseJob.dataSizeThreshold,
            dropIfExists: baseJob.dropIfExists,
            parallel: baseJob.parallel
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: cloneJob)
        #expect(script.contains("products"))
        #expect(script.contains("order_status"))
    }

    // MARK: - Schema Diff Edge Cases

    @Test("Diff detects view definition differences")
    func diffViewDefinitions() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).diff_view CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_base CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).diff_base (id integer PRIMARY KEY, name text, active boolean DEFAULT true)
            """, on: conn)
        }

        try await IntegrationTestConfig.execute("""
            CREATE VIEW \(Self.testSchema).diff_view AS SELECT id, name FROM \(Self.testSchema).diff_base WHERE active = true
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE VIEW \(Self.testSchema).diff_view AS SELECT id, name FROM \(Self.testSchema).diff_base
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let viewId = ObjectIdentifier(type: .view, schema: Self.testSchema, name: "diff_view")
        let objDiff = try await differ.compareObjects(viewId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Views with different definitions should show as modified")

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).diff_view CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_base CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Diff detects enum label differences")
    func diffEnumLabels() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).diff_enum CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).diff_enum CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("CREATE TYPE \(Self.testSchema).diff_enum AS ENUM ('a', 'b', 'c')", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE TYPE \(Self.testSchema).diff_enum AS ENUM ('a', 'b')", on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let enumId = ObjectIdentifier(type: .enum, schema: Self.testSchema, name: "diff_enum")
        let objDiff = try await differ.compareObjects(enumId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Enums with different labels should show as modified")

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).diff_enum CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).diff_enum CASCADE", on: targetConn)
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Diff detects sequence parameter differences")
    func diffSequenceParameters() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).diff_seq CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).diff_seq CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("CREATE SEQUENCE \(Self.testSchema).diff_seq START 100 INCREMENT 5", on: sourceConn)
        try await IntegrationTestConfig.execute("CREATE SEQUENCE \(Self.testSchema).diff_seq START 1 INCREMENT 1", on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let seqId = ObjectIdentifier(type: .sequence, schema: Self.testSchema, name: "diff_seq")
        let objDiff = try await differ.compareObjects(seqId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Sequences with different parameters should show as modified")

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).diff_seq CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).diff_seq CASCADE", on: targetConn)
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Diff detects function definition differences")
    func diffFunctionDefinitions() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).diff_func(integer) CASCADE", on: conn)
        }

        try await IntegrationTestConfig.execute("""
            CREATE FUNCTION \(Self.testSchema).diff_func(x integer) RETURNS integer AS $$ SELECT x * 2; $$ LANGUAGE sql IMMUTABLE
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE FUNCTION \(Self.testSchema).diff_func(x integer) RETURNS integer AS $$ SELECT x + 1; $$ LANGUAGE sql STABLE
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let funcId = ObjectIdentifier(type: .function, schema: Self.testSchema, name: "diff_func", signature: "(integer)")
        let objDiff = try await differ.compareObjects(funcId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Functions with different bodies/volatility should show as modified")

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).diff_func(integer) CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Diff of matching objects returns nil (no differences)")
    func diffMatchingObjects() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).identical_tbl CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).identical_tbl (
                    id integer PRIMARY KEY,
                    name text NOT NULL,
                    value numeric(10,2) DEFAULT 0
                )
            """, on: conn)
        }

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "identical_tbl")
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff == nil, "Identical tables should have no differences")

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).identical_tbl CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    // MARK: - Error Scenarios

    @Test("Clone of nonexistent object throws error")
    func cloneNonexistentObject() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "totally_nonexistent_table_xyz"))
            ],
            dryRun: true,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        do {
            _ = try await orchestrator.execute(job: job)
            #expect(Bool(false), "Should have thrown for nonexistent object")
        } catch {
            #expect(error is PGSchemaEvoError)
        }
    }

    // MARK: - Sync dry-run tests

    @Test("Sync generates CREATE for missing enum on target")
    func syncCreatesMissingEnum() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_enum_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_enum_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("CREATE TYPE \(Self.testSchema).sync_enum_test AS ENUM ('red', 'green', 'blue')", on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let enumId = ObjectIdentifier(type: .enum, schema: Self.testSchema, name: "sync_enum_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: enumId)],
            dryRun: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await syncOrchestrator.execute(job: syncJob)

        #expect(script.contains("CREATE TYPE") && script.contains("sync_enum_test"), "Should generate CREATE TYPE for missing enum")

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_enum_test CASCADE", on: sc)
        try? await sc.close()
    }

    @Test("Sync of matching objects reports no changes needed")
    func syncAlreadyInSync() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).in_sync_tbl CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).in_sync_tbl (id integer PRIMARY KEY, data text)
            """, on: conn)
        }

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "in_sync_tbl")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await syncOrchestrator.execute(job: syncJob)
        #expect(result.contains("already in sync") || result.contains("No changes"))

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).in_sync_tbl CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).in_sync_tbl CASCADE", on: tc)
        try? await sc.close()
        try? await tc.close()
    }

    // MARK: - Dry-Run: Multiple data methods

    @Test("Dry-run with pg_dump data method generates pg_dump commands")
    func dryRunPgDumpMethod() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyData: true
                ),
            ],
            dryRun: true,
            defaultDataMethod: .pgDump,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("pg_dump") || script.contains("pg_restore"), "pg_dump method should generate pg_dump/pg_restore commands")
    }

    @Test("Dry-run with copy method generates COPY commands")
    func dryRunCopyMethod() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyData: true
                ),
            ],
            dryRun: true,
            defaultDataMethod: .copy,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("\\copy"), "copy method should generate \\copy commands")
    }
}
