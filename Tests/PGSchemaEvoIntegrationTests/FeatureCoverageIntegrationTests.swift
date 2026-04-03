import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

/// Integration tests covering user-facing feature gaps:
/// - Materialized view sync & diff
/// - Role sync
/// - Full end-to-end data sync workflow
/// - Clone with retry
/// - Config-file driven clone
/// - Multi-schema clone & diff
@Suite("Feature Coverage Integration Tests", .serialized)
struct FeatureCoverageIntegrationTests {

    /// Dedicated schema for test-specific tables to avoid cross-suite race conditions.
    /// Other suites scan `public` tables, so ephemeral tables there cause objectNotFound races.
    static let testSchema = "fc_test"

    /// Ensure the dedicated test schema exists on a connection.
    static func ensureTestSchema(on connection: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: connection)
    }

    // MARK: - 1. Materialized View Sync & Diff

    @Test("Sync creates missing materialized view on target")
    func syncMaterializedView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Ensure analytics schema exists on target (matview depends on it)
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS analytics", on: targetConn)
        // Ensure prerequisite tables exist on target for the matview definition
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)

        // Also need orders table on target for the matview query to work at creation
        // We'll use dry-run to avoid needing the full dependency chain
        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary")),
            ],
            dryRun: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Verify the script contains CREATE MATERIALIZED VIEW
        #expect(script.contains("daily_order_summary"), "Script should reference the materialized view")
        #expect(script.contains("CREATE"), "Script should contain CREATE for missing matview on target")
    }

    @Test("Diff detects materialized view only in source")
    func diffMaterializedView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        // Ensure target has analytics schema but no matview
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS analytics", on: targetConn)
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let diff = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: "analytics",
            types: [.materializedView]
        )

        #expect(diff.onlyInSource.count >= 1, "Matview should be only in source")
        let matviewId = diff.onlyInSource.first { $0.name == "daily_order_summary" }
        #expect(matviewId != nil, "daily_order_summary should be in onlyInSource")

        let rendered = diff.renderText()
        #expect(rendered.contains("daily_order_summary"), "Rendered diff should mention the matview")
    }

    @Test("Diff detects modified materialized view definition")
    func diffModifiedMaterializedView() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        // Create a different matview on target with same name but different query
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS analytics", on: targetConn)
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE MATERIALIZED VIEW analytics.daily_order_summary AS
            SELECT current_date AS order_date, 0 AS order_count, 0::numeric AS total_revenue
            WITH DATA
            """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let matviewId = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "daily_order_summary")
        let objDiff = try await differ.compareObjects(matviewId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Should detect differences in matview definitions")
        if let diff = objDiff {
            #expect(!diff.differences.isEmpty, "Should have at least one difference")
        }

        // Cleanup
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)
    }

    // MARK: - 2. Role Sync

    @Test("Sync creates missing role on target via dry-run")
    func syncRole() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        // Create a test-specific role on source to avoid cross-suite race on readonly_role
        let roleName = "fc_test_sync_role"
        try await IntegrationTestConfig.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '\(roleName)') THEN
                    CREATE ROLE \(roleName) NOLOGIN;
                END IF;
            END $$
            """, on: sourceConn)
        // Ensure role does NOT exist on target
        try await IntegrationTestConfig.execute("DROP ROLE IF EXISTS \(roleName)", on: targetConn)

        let job = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .role, schema: nil, name: roleName)),
            ],
            dryRun: true,
            force: true,
            skipPreflight: true
        )

        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains(roleName), "Script should reference the role")
        #expect(script.contains("CREATE") || script.contains("ROLE"), "Script should contain CREATE ROLE")

        // Cleanup
        try await IntegrationTestConfig.execute("DROP ROLE IF EXISTS \(roleName)", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP ROLE IF EXISTS \(roleName)", on: targetConn)
    }

    // MARK: - 3. Full End-to-End Data Sync Workflow

    @Test("Data sync full workflow: init, insert, sync, verify")
    func dataSyncEndToEnd() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let shell = ShellRunner()
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(command: "psql", exitCode: -1, stderr: "psql not found")
        }
        let sourceDSN = sourceConfig.toDSN()
        let targetDSN = targetConfig.toDSN()
        let sourceEnv = sourceConfig.environment()
        let targetEnv = targetConfig.environment()

        let schema = Self.testSchema
        let tableName = "e2e_sync_test"
        let qualifiedName = "\(schema).\(tableName)"

        // Step 1: Create schema and identical tables on source and target
        let ddl = """
            CREATE SCHEMA IF NOT EXISTS \(schema);
            DROP TABLE IF EXISTS \(qualifiedName) CASCADE;
            CREATE TABLE \(qualifiedName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                amount numeric(10,2) NOT NULL DEFAULT 0,
                updated_at timestamp with time zone NOT NULL DEFAULT now()
            );
            """
        _ = try await shell.run(command: psqlPath, arguments: [sourceDSN, "-c", ddl], environment: sourceEnv)
        _ = try await shell.run(command: psqlPath, arguments: [targetDSN, "-c", ddl], environment: targetEnv)

        // Step 2: Insert initial data on both source and target
        let initialData = """
            INSERT INTO \(qualifiedName) (id, name, amount, updated_at) VALUES
                (1, 'Alice', 100.00, '2026-01-01 00:00:00+00'),
                (2, 'Bob', 200.00, '2026-01-02 00:00:00+00');
            """
        _ = try await shell.run(command: psqlPath, arguments: [sourceDSN, "-c", initialData], environment: sourceEnv)
        _ = try await shell.run(command: psqlPath, arguments: [targetDSN, "-c", initialData], environment: targetEnv)

        // Step 3: Initialize data sync state
        let stateFile = NSTemporaryDirectory() + "fc-e2e-sync-\(UUID().uuidString).yaml"
        let initJob = DataSyncJob(
            source: sourceConfig,
            target: targetConfig,
            tables: [
                DataSyncTableConfig(
                    id: ObjectIdentifier(type: .table, schema: schema, name: tableName),
                    trackingColumn: "updated_at"
                ),
            ],
            stateFilePath: stateFile
        )

        let orchestrator = DataSyncOrchestrator(logger: IntegrationTestConfig.logger)
        let initOutput = try await orchestrator.initialize(job: initJob)
        #expect(initOutput.contains(tableName), "Init should reference the table")

        // Verify state was captured
        let stateStore = DataSyncStateStore()
        let state = try stateStore.load(path: stateFile)
        #expect(state.tables[qualifiedName] != nil, "State should track the table")

        // Step 4: Make changes on source — update a row and insert a new one
        _ = try await shell.run(
            command: psqlPath,
            arguments: [sourceDSN, "-c", """
                UPDATE \(qualifiedName) SET name = 'Alice Updated', amount = 150.00, updated_at = '2026-03-01 00:00:00+00' WHERE id = 1;
                INSERT INTO \(qualifiedName) (id, name, amount, updated_at) VALUES (3, 'Charlie', 300.00, '2026-03-01 00:00:00+00');
                """],
            environment: sourceEnv
        )

        // Step 5: Run incremental sync
        let runJob = DataSyncJob(
            source: sourceConfig,
            target: targetConfig,
            tables: [],
            stateFilePath: stateFile,
            force: true
        )
        let syncOutput = try await orchestrator.run(job: runJob)
        #expect(syncOutput.contains("synced"), "Sync should report rows synced")

        // Step 6: Verify target has the updates
        let verifyResult = try await shell.run(
            command: psqlPath,
            arguments: [targetDSN, "-t", "-A", "-c", "SELECT id, name, amount FROM \(qualifiedName) ORDER BY id;"],
            environment: targetEnv
        )
        let lines = verifyResult.stdout.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\n")
        #expect(lines.count == 3, "Target should have 3 rows after sync")
        #expect(lines[0].contains("Alice Updated"), "Row 1 should be updated")
        #expect(lines[0].contains("150"), "Row 1 amount should be updated")
        #expect(lines[2].contains("Charlie"), "Row 3 (new) should be synced")

        // Cleanup
        _ = try await shell.run(command: psqlPath, arguments: [sourceDSN, "-c", "DROP TABLE IF EXISTS \(qualifiedName) CASCADE;"], environment: sourceEnv)
        _ = try await shell.run(command: psqlPath, arguments: [targetDSN, "-c", "DROP TABLE IF EXISTS \(qualifiedName) CASCADE;"], environment: targetEnv)
    }

    // MARK: - 4. Clone with Retry

    @Test("Clone with retries succeeds on first attempt")
    func cloneWithRetry() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
            ],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 2,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify sequence exists on target
        let rows = try await targetConn.query(
            PostgresQuery(unsafeSQL: "SELECT sequencename FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'invoice_number_seq'"),
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows { found = true }
        #expect(found, "Sequence should exist on target after clone with retries")

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS public.invoice_number_seq CASCADE", on: targetConn)
    }

    // MARK: - 5. Config-File Driven Clone

    @Test("Clone job loaded from YAML config file executes correctly")
    func configFileDrivenClone() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Create a YAML config file
        let configPath = NSTemporaryDirectory() + "clone-config-\(UUID().uuidString).yaml"
        let yamlContent = """
            source:
              dsn: "\(sourceConfig.toDSN())"
            target:
              dsn: "\(targetConfig.toDSN())"
            defaults:
              drop_existing: true
            objects:
              - type: sequence
                schema: public
                name: invoice_number_seq
              - type: enum
                schema: public
                name: user_role
            """
        try yamlContent.write(toFile: configPath, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: configPath) }

        // Load config and convert to CloneJob
        let loader = ConfigLoader()
        let config = try loader.load(
            path: configPath,
            overrides: ConfigOverrides(dryRun: true, force: true)
        )

        #expect(config.objects.count == 2, "Config should have 2 objects")
        #expect(config.dropIfExists, "drop_existing should be true from defaults")

        // Execute via orchestrator
        let job = config.toCloneJob()
        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("invoice_number_seq"), "Script should reference the sequence")
        #expect(script.contains("user_role"), "Script should reference the enum")
    }

    @Test("Config-file driven clone with data and permissions")
    func configFileDrivenCloneWithData() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let configPath = NSTemporaryDirectory() + "clone-data-config-\(UUID().uuidString).yaml"
        let yamlContent = """
            source:
              dsn: "\(sourceConfig.toDSN())"
            target:
              dsn: "\(targetConfig.toDSN())"
            defaults:
              data: true
              permissions: true
              drop_existing: true
            objects:
              - type: table
                schema: public
                name: products
                where: "price > 10"
                row_limit: 5
            """
        try yamlContent.write(toFile: configPath, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: configPath) }

        let loader = ConfigLoader()
        let config = try loader.load(
            path: configPath,
            overrides: ConfigOverrides(dryRun: true, force: true)
        )

        #expect(config.objects.count == 1)
        let spec = config.objects[0]
        #expect(spec.copyData, "data should be enabled")
        #expect(spec.copyPermissions, "permissions should be enabled")
        #expect(spec.whereClause == "price > 10", "WHERE clause should be parsed")
        #expect(spec.rowLimit == 5, "row_limit should be parsed")

        let job = config.toCloneJob()
        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("products"), "Script should reference products table")
    }

    // MARK: - 6. Multi-Schema Clone & Diff

    @Test("Clone objects from multiple schemas in a single job")
    func multiSchemaClone() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer { Task { try? await targetConn.close() } }

        // Ensure analytics schema exists on target
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS analytics", on: targetConn)

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                // public schema object
                ObjectSpec(id: ObjectIdentifier(type: .sequence, schema: "public", name: "invoice_number_seq")),
                // analytics schema object
                ObjectSpec(id: ObjectIdentifier(type: .schema, schema: nil, name: "analytics")),
            ],
            dryRun: true,
            dropIfExists: true,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("invoice_number_seq"), "Script should contain public schema object")
        #expect(script.contains("analytics"), "Script should contain analytics schema object")
    }

    @Test("Diff across multiple schemas detects differences")
    func multiSchemaDiff() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        // Ensure analytics schema on target but drop the matview
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS analytics", on: targetConn)
        try await IntegrationTestConfig.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_order_summary CASCADE", on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)
        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)

        // Diff analytics schema matviews (analytics schema is not touched by other suites)
        let analyticsDiff = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: "analytics",
            types: [.materializedView]
        )

        // Also diff our dedicated fc_test schema to exercise multi-schema diffing
        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)
        // Create a table only on source in fc_test schema
        let tableName = "diff_only_source"
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(tableName) CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).\(tableName) (id int PRIMARY KEY)
            """, on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(tableName) CASCADE", on: targetConn)

        let fcDiff = try await differ.diff(
            source: sourceIntrospector,
            target: targetIntrospector,
            schema: Self.testSchema,
            types: [.table]
        )

        // analytics.daily_order_summary should be only in source
        #expect(analyticsDiff.onlyInSource.count >= 1, "Analytics matview should be only in source")
        let matviewFound = analyticsDiff.onlyInSource.contains { $0.name == "daily_order_summary" }
        #expect(matviewFound, "daily_order_summary should be in onlyInSource for analytics schema")

        // fc_test table should be only in source
        #expect(fcDiff.onlyInSource.count >= 1, "fc_test table should be only in source")

        // Render output to exercise multi-schema diff rendering
        let analyticsText = analyticsDiff.renderText()
        #expect(analyticsText.contains("daily_order_summary"), "Analytics diff should mention matview")

        // Cleanup
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(tableName) CASCADE", on: sourceConn)
    }

    @Test("Diff generates migration SQL across schemas")
    func multiSchemaDiffMigrationSQL() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)
        defer {
            Task { try? await sourceConn.close() }
            Task { try? await targetConn.close() }
        }

        // Use dedicated schema to avoid cross-suite race on public tables
        let schema = Self.testSchema
        let tableName = "diff_migration_test"
        let qualifiedName = "\(schema).\(tableName)"

        // Ensure schema exists
        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        // Create table with extra column on source
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(qualifiedName) CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(qualifiedName) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL,
                description text,
                price numeric(10, 2) NOT NULL DEFAULT 0
            )
            """, on: sourceConn)

        // Create same table on target but with fewer columns
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(qualifiedName) CASCADE", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(qualifiedName) (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name text NOT NULL
            )
            """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)
        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)

        let tableId = ObjectIdentifier(type: .table, schema: schema, name: tableName)
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Should detect column differences in table")
        if let diff = objDiff {
            #expect(!diff.migrationSQL.isEmpty, "Should generate migration SQL for missing columns")
            let sql = diff.migrationSQL.joined(separator: "\n")
            #expect(sql.contains("ALTER TABLE") || sql.contains("ADD COLUMN"), "Migration SQL should contain ALTER/ADD")
        }

        // Cleanup
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(qualifiedName) CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(qualifiedName) CASCADE", on: targetConn)
    }
}
