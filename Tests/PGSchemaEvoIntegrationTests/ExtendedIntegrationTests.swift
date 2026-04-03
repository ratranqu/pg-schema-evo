import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Extended Integration Tests", .tags(.integration), .serialized)
struct ExtendedIntegrationTests {

    private static let testSchema = "_ext_test"

    private static func ensureCleanTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("DROP SCHEMA IF EXISTS \(testSchema) CASCADE", on: conn)
        try await IntegrationTestConfig.execute("CREATE SCHEMA \(testSchema)", on: conn)
    }

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - YAML Config File Loading (end-to-end)

    @Test("Config loader parses YAML and produces valid CloneJobConfig")
    func configLoaderEndToEnd() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        // Write a temporary YAML config file
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
        // Override to dry-run for safety
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
        // Override drop_existing via CLI
        let overrides = ConfigOverrides(dropExisting: true, force: true)
        let config = try loader.load(path: configPath, overrides: overrides)

        #expect(config.dropIfExists == true, "CLI override should win over config file")
    }

    @Test("Config loader rejects missing config file")
    func configLoaderMissingFile() async throws {
        let loader = ConfigLoader()
        #expect(throws: PGSchemaEvoError.self) {
            _ = try loader.load(path: "/nonexistent/path/config.yaml")
        }
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

        // Create base table on both
        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).diff_view CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_base CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).diff_base (id integer PRIMARY KEY, name text, active boolean DEFAULT true)
            """, on: conn)
        }

        // Different view definitions
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
        if let diff = objDiff {
            #expect(!diff.differences.isEmpty)
            let hasDefDiff = diff.differences.contains { $0.lowercased().contains("definition") || $0.lowercased().contains("view") }
            #expect(hasDefDiff, "Should detect view definition difference")
        }

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP VIEW IF EXISTS \(Self.testSchema).diff_view CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_base CASCADE", on: conn)
        }
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
        if let diff = objDiff {
            #expect(!diff.differences.isEmpty)
        }

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).diff_seq CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).diff_seq CASCADE", on: targetConn)
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
        if let diff = objDiff {
            let hasLabelDiff = diff.differences.contains { $0.contains("c") || $0.contains("label") || $0.contains("enum") }
            #expect(hasLabelDiff, "Should detect missing enum label 'c'")
        }

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).diff_enum CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).diff_enum CASCADE", on: targetConn)
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

        // Create identical tables on both
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

    @Test("Full schema diff reports counts correctly")
    func fullSchemaDiffCounts() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Clean slate: drop and recreate schema to avoid stale objects from other tests
        try await Self.ensureCleanTestSchema(on: sourceConn)
        try await Self.ensureCleanTestSchema(on: targetConn)

        // Shared table on both
        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).diff_shared (id integer PRIMARY KEY)", on: conn)
        }
        // Source-only
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).diff_source_only (id integer PRIMARY KEY)", on: sourceConn)
        // Target-only
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).diff_target_only (id integer PRIMARY KEY)", on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let result = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: Self.testSchema,
            types: [.table]
        )

        #expect(result.onlyInSource.count == 1, "Should have 1 source-only table")
        #expect(result.onlyInTarget.count == 1, "Should have 1 target-only table")
        #expect(result.matching >= 1, "Should have at least 1 matching table")

        let sourceOnlyNames = result.onlyInSource.map(\.name)
        #expect(sourceOnlyNames.contains("diff_source_only"))
        let targetOnlyNames = result.onlyInTarget.map(\.name)
        #expect(targetOnlyNames.contains("diff_target_only"))

        // Verify text rendering includes summary
        let text = result.renderText()
        #expect(text.contains("Summary:"))

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_shared CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_source_only CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).diff_target_only CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    // MARK: - Error Scenarios

    @Test("Clone of nonexistent object throws objectNotFound error")
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
            // Should get objectNotFound or similar error
            #expect(error is PGSchemaEvoError)
        }
    }

    @Test("Preflight catches nonexistent source objects")
    func preflightNonexistentSource() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "preflight_nonexist_xyz"))
            ],
            dryRun: false,
            force: true,
            skipPreflight: false
        )

        let checker = PreflightChecker(logger: IntegrationTestConfig.logger)
        let failures = try await checker.check(job: job)
        #expect(failures.contains { $0.contains("preflight_nonexist_xyz") })
    }

    // MARK: - Sync: dropExtra flag

    @Test("Sync with dropExtra removes objects only on target")
    func syncDropExtra() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create a table only on target (not on source)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).extra_target CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).extra_target CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).extra_target (id integer PRIMARY KEY)", on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "extra_target")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            dropExtra: true,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify table was dropped from target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.tables WHERE table_schema = '\(Self.testSchema)' AND table_name = 'extra_target'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 0, "Table should be dropped from target")
        }
        try? await vc.close()
    }

    @Test("Sync without dropExtra preserves extra target objects")
    func syncPreservesExtra() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).keep_extra CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).keep_extra CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("CREATE TABLE \(Self.testSchema).keep_extra (id integer PRIMARY KEY)", on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "keep_extra")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            dropExtra: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Table should still exist
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.tables WHERE table_schema = '\(Self.testSchema)' AND table_name = 'keep_extra'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count == 1, "Table should be preserved when dropExtra is false")
        }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).keep_extra CASCADE", on: vc)
        try? await vc.close()
    }

    // MARK: - Live Clone: Function

    @Test("Live clone of function creates function on target")
    func liveCloneFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Create a self-contained function in isolated schema (no external table deps)
        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).ext_clone_func(integer) CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).ext_clone_func(integer) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE FUNCTION \(Self.testSchema).ext_clone_func(x integer) RETURNS integer AS $$ SELECT x * 7; $$ LANGUAGE sql IMMUTABLE
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .function, schema: Self.testSchema, name: "ext_clone_func", signature: "(integer)")
                ),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify function exists on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = '\(Self.testSchema)' AND p.proname = 'ext_clone_func'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count >= 1, "Function should exist on target after clone")
        }

        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).ext_clone_func(integer) CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).ext_clone_func(integer) CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Data Integrity After Clone

    @Test("Clone with data preserves exact row data")
    func cloneDataIntegrity() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).data_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).data_test CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).data_test (
                id integer PRIMARY KEY,
                name text NOT NULL,
                amount numeric(10,2)
            )
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            INSERT INTO \(Self.testSchema).data_test (id, name, amount) VALUES
            (1, 'Alice', 100.50), (2, 'Bob', 200.75), (3, 'Charlie', 0.01)
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: Self.testSchema, name: "data_test"),
                    copyData: true
                ),
            ],
            dryRun: false,
            defaultDataMethod: .copy,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify exact row count and data
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let countRows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM \(Self.testSchema).data_test"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in countRows {
            let count = try row.decode(Int.self)
            #expect(count == 3, "Should have exactly 3 rows")
        }

        // Verify specific values
        let nameRows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT name FROM \(Self.testSchema).data_test ORDER BY id"),
            logger: IntegrationTestConfig.logger
        )
        var names: [String] = []
        for try await row in nameRows {
            names.append(try row.decode(String.self))
        }
        #expect(names == ["Alice", "Bob", "Charlie"])

        // Clean up
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).data_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).data_test CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sync: Sequence and Enum Differences

    @Test("Sync creates missing enum on target")
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
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify enum was created on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = '\(Self.testSchema)' AND t.typname = 'sync_enum_test' ORDER BY e.enumsortorder"),
            logger: IntegrationTestConfig.logger
        )
        var labels: [String] = []
        for try await row in rows {
            labels.append(try row.decode(String.self))
        }
        #expect(labels == ["red", "green", "blue"])

        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_enum_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).sync_enum_test CASCADE", on: sc)
        try? await sc.close()
    }

    @Test("Sync creates missing sequence on target")
    func syncCreatesMissingSequence() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("CREATE SEQUENCE \(Self.testSchema).sync_seq_test START 500 INCREMENT 10", on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let seqId = ObjectIdentifier(type: .sequence, schema: Self.testSchema, name: "sync_seq_test")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: seqId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify sequence was created on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT start_value FROM information_schema.sequences WHERE sequence_schema = '\(Self.testSchema)' AND sequence_name = 'sync_seq_test'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let startValue = try row.decode(String.self)
            #expect(startValue == "500")
        }

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).sync_seq_test CASCADE", on: sc)
        try? await sc.close()
    }

    @Test("Sync creates missing function on target")
    func syncCreatesMissingFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test(integer) CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test(integer) CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE FUNCTION \(Self.testSchema).sync_func_test(x integer) RETURNS integer AS $$ SELECT x * 3; $$ LANGUAGE sql IMMUTABLE
        """, on: sourceConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let funcId = ObjectIdentifier(type: .function, schema: Self.testSchema, name: "sync_func_test", signature: "(integer)")
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: funcId)],
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await syncOrchestrator.execute(job: syncJob)

        // Verify function exists on target
        let vc = try await IntegrationTestConfig.connect(to: targetConfig)
        let rows = try await vc.query(
            PostgresQuery(unsafeSQL: "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = '\(Self.testSchema)' AND p.proname = 'sync_func_test'"),
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let count = try row.decode(Int.self)
            #expect(count >= 1, "Function should exist on target after sync")
        }

        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test(integer) CASCADE", on: vc)
        try? await vc.close()

        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).sync_func_test(integer) CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sync: Already in sync

    @Test("Sync of matching objects reports no changes needed")
    func syncAlreadyInSync() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create identical tables
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
            dryRun: false,
            force: true
        )

        let syncOrchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let result = try await syncOrchestrator.execute(job: syncJob)
        #expect(result.contains("already in sync") || result.contains("No changes"))

        // Clean up
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
