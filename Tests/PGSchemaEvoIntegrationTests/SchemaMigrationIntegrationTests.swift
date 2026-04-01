import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Schema Migration Integration Tests", .tags(.integration), .serialized)
struct SchemaMigrationIntegrationTests {

    // Use a unique table name to avoid conflicts with other test suites
    private static let tableName = "mig_test_tbl"
    // Use a dedicated schema on SOURCE to avoid interfering with Phase4's public schema scan
    private static let testSchema = "_mig_test"

    /// Ensure the test schema exists on both databases.
    private static func ensureSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    /// Create the "source" version of the test table on the source database in the test schema.
    private static func createSourceTable(on conn: PostgresConnection) async throws {
        try await ensureSchema(on: conn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).\(tableName) CASCADE", on: conn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(testSchema).\(tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                description text,
                price numeric(10,2) NOT NULL DEFAULT 0,
                stock_count integer NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            )
        """, on: conn)
    }

    /// Create the target table in the test schema on the target database.
    private static func createTargetTable(sql: String, on conn: PostgresConnection) async throws {
        try await ensureSchema(on: conn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).\(tableName) CASCADE", on: conn)
        try await IntegrationTestConfig.execute(sql, on: conn)
    }

    /// Clean up the test table from both databases using fresh connections.
    private static func cleanup() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).\(tableName) CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).\(tableName) CASCADE", on: tc)
        try? await sc.close()
        try? await tc.close()
    }

    /// Full cleanup: drop all test objects from both databases.
    private static func fullCleanup() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        // Drop all test tables in the test schema
        for tbl in [tableName, "rls_test", "trigger_test", "rls_diff_test"] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).\(tbl) CASCADE", on: sc)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(testSchema).\(tbl) CASCADE", on: tc)
        }
        // Drop test functions
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(testSchema).trg_test_func() CASCADE", on: sc)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(testSchema).trg_test_func() CASCADE", on: tc)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(testSchema).noop_trigger() CASCADE", on: tc)
        try? await sc.close()
        try? await tc.close()
    }

    /// Open a fresh connection to the target for verification queries.
    private static func freshTargetConn() async throws -> PostgresConnection {
        try await IntegrationTestConfig.connect(to: try IntegrationTestConfig.targetConfig())
    }

    // MARK: - Sync: allowDropColumns

    @Test("Sync skips DROP COLUMN when allowDropColumns is false")
    func syncSkipsDropColumnWithoutFlag() async throws {
        // Pre-clean to handle any leftover state
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                price numeric(10,2) NOT NULL DEFAULT 0,
                legacy_field text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            allowDropColumns: false,
            force: true
        )
        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        // Verify with fresh connection
        let vc = try await Self.freshTargetConn()
        let rows = try await vc.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = \(Self.testSchema) AND table_name = \(Self.tableName) AND column_name = 'legacy_field'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found, "legacy_field should still exist when allowDropColumns is false")
        try? await vc.close()

        try await Self.cleanup()
    }

    @Test("Sync drops extra column when allowDropColumns is true")
    func syncDropsColumnWithFlag() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                price numeric(10,2) NOT NULL DEFAULT 0,
                legacy_field text
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            allowDropColumns: true,
            force: true
        )
        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        let vc = try await Self.freshTargetConn()
        let rows = try await vc.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = \(Self.testSchema) AND table_name = \(Self.tableName) AND column_name = 'legacy_field'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(!found, "legacy_field should be dropped when allowDropColumns is true")
        try? await vc.close()

        try await Self.cleanup()
    }

    @Test("Sync drops extra constraint when allowDropColumns is true")
    func syncDropsExtraConstraint() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                description text,
                price numeric(10,2) NOT NULL DEFAULT 0,
                stock_count integer NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                CONSTRAINT chk_legacy_stock CHECK (stock_count >= 0)
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            allowDropColumns: true,
            force: true
        )
        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        let vc = try await Self.freshTargetConn()
        let rows = try await vc.query(
            "SELECT conname FROM pg_constraint WHERE conrelid = '_mig_test.mig_test_tbl'::regclass AND conname = 'chk_legacy_stock'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(!found, "Extra constraint should be dropped when allowDropColumns is true")
        try? await vc.close()

        try await Self.cleanup()
    }

    // MARK: - Sync: modified table (column changes)

    @Test("Sync adds missing column to target table")
    func syncAddsMissingColumn() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                price numeric(10,2) NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true
        )
        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        let vc = try await Self.freshTargetConn()
        let rows = try await vc.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = \(Self.testSchema) AND table_name = \(Self.tableName) AND column_name = 'stock_count'",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await _ in rows {
            found = true
        }
        #expect(found, "Missing column stock_count should be added by sync")
        try? await vc.close()

        try await Self.cleanup()
    }

    @Test("Sync alters column type on target table")
    func syncAltersColumnType() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                description varchar(100),
                price numeric(10,2) NOT NULL DEFAULT 0,
                stock_count integer NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            )
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            force: true
        )
        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        let vc = try await Self.freshTargetConn()
        let rows = try await vc.query(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = \(Self.testSchema) AND table_name = \(Self.tableName) AND column_name = 'description'",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rows {
            let dataType = try row.decode(String.self)
            #expect(dataType == "text", "Column type should be altered to match source")
        }
        try? await vc.close()

        try await Self.cleanup()
    }

    // MARK: - Diff: --sql output

    @Test("Diff renderMigrationSQL produces valid SQL for table differences")
    func diffSQLForTableDifferences() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                price numeric(10,2) NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            )
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil)
        #expect(!objDiff!.migrationSQL.isEmpty)
        let sql = objDiff!.migrationSQL.joined(separator: "\n")
        #expect(sql.contains("ADD COLUMN"))

        try? await sourceConn.close()
        try? await targetConn.close()
        try await Self.cleanup()
    }

    @Test("Diff renderMigrationSQL includes destructive changes when requested")
    func diffSQLDestructiveFlag() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                description text,
                price numeric(10,2) NOT NULL DEFAULT 0,
                stock_count integer NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                legacy_col text
            )
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil)
        #expect(!objDiff!.dropColumnSQL.isEmpty)
        let dropSQL = objDiff!.dropColumnSQL.joined(separator: "\n")
        #expect(dropSQL.contains("DROP COLUMN") && dropSQL.contains("legacy_col"))

        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [],
            modified: [objDiff!],
            matching: 0
        )

        let sqlDefault = diff.renderMigrationSQL()
        #expect(sqlDefault.contains("SKIPPED"))

        let sqlDestructive = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sqlDestructive.contains("DROP COLUMN"))
        #expect(!sqlDestructive.contains("SKIPPED"))

        try? await sourceConn.close()
        try? await targetConn.close()
        try await Self.cleanup()
    }

    // MARK: - RLS policy cloning

    @Test("RLS introspection returns correct policies for source table")
    func rlsIntrospectionCorrectPolicies() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let usersId = ObjectIdentifier(type: .table, schema: "public", name: "users")

        let rlsInfo = try await introspector.rlsPolicies(for: usersId)

        #expect(rlsInfo.isEnabled, "RLS should be enabled on source users table")
        #expect(rlsInfo.policies.count >= 2, "Should have at least 2 policies")

        let policyNames = rlsInfo.policies.map(\.name)
        #expect(policyNames.contains("users_self_access"))
        #expect(policyNames.contains("users_admin_all"))

        for policy in rlsInfo.policies {
            #expect(policy.definition.contains("CREATE POLICY"), "Policy definition should start with CREATE POLICY")
        }

        try? await sourceConn.close()
    }

    @Test("Clone table with simple RLS policy preserves it on target")
    func cloneWithSimpleRLSPolicy() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        // Use test schema to avoid interfering with other suites
        try await Self.ensureSchema(on: sourceConn)
        try await Self.ensureSchema(on: targetConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).rls_test CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).rls_test (
                id integer PRIMARY KEY,
                data text
            )
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("ALTER TABLE \(Self.testSchema).rls_test ENABLE ROW LEVEL SECURITY", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE POLICY rls_test_select ON \(Self.testSchema).rls_test FOR SELECT USING (true)
        """, on: sourceConn)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).rls_test CASCADE", on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: Self.testSchema, name: "rls_test"),
                    copyRLSPolicies: true
                ),
            ],
            dryRun: false,
            force: true,
            retries: 0,
            skipPreflight: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: job)

        // Verify with fresh connection
        let vc = try await Self.freshTargetConn()

        let rlsRows = try await vc.query(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'rls_test' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = \(Self.testSchema))",
            logger: IntegrationTestConfig.logger
        )
        for try await row in rlsRows {
            let enabled = try row.decode(Bool.self)
            #expect(enabled, "RLS should be enabled on cloned table")
        }

        let policyRows = try await vc.query(
            "SELECT polname FROM pg_policy pol JOIN pg_class c ON c.oid = pol.polrelid WHERE c.relname = 'rls_test' AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = \(Self.testSchema))",
            logger: IntegrationTestConfig.logger
        )
        var found = false
        for try await row in policyRows {
            let name = try row.decode(String.self)
            #expect(name == "rls_test_select")
            found = true
        }
        #expect(found, "RLS policy should exist on target")
        try? await vc.close()

        // Clean up with fresh connections
        let sc2 = try await IntegrationTestConfig.connect(to: sourceConfig)
        let tc2 = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).rls_test CASCADE", on: sc2)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).rls_test CASCADE", on: tc2)
        try? await sc2.close()
        try? await tc2.close()
    }

    // MARK: - Trigger comparison (live sync)

    @Test("Diff detects trigger differences between source and target")
    func diffDetectsTriggerDifferences() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        let triggerTable = "trigger_test"
        try await Self.ensureSchema(on: sourceConn)
        try await Self.ensureSchema(on: targetConn)
        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(triggerTable) CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).\(triggerTable) (
                    id integer PRIMARY KEY,
                    data text
                )
            """, on: conn)
        }

        try await IntegrationTestConfig.execute("""
            CREATE OR REPLACE FUNCTION \(Self.testSchema).trg_test_func() RETURNS trigger AS $$
            BEGIN RETURN NEW; END;
            $$ LANGUAGE plpgsql
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TRIGGER trg_source_only
                BEFORE INSERT ON \(Self.testSchema).\(triggerTable)
                FOR EACH ROW EXECUTE FUNCTION \(Self.testSchema).trg_test_func()
        """, on: sourceConn)

        try await IntegrationTestConfig.execute("""
            CREATE OR REPLACE FUNCTION \(Self.testSchema).trg_test_func() RETURNS trigger AS $$
            BEGIN RETURN NEW; END;
            $$ LANGUAGE plpgsql
        """, on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TRIGGER trg_target_only
                BEFORE UPDATE ON \(Self.testSchema).\(triggerTable)
                FOR EACH ROW EXECUTE FUNCTION \(Self.testSchema).trg_test_func()
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: triggerTable)
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Should detect trigger differences")
        if let diff = objDiff {
            #expect(diff.differences.contains { $0.contains("trg_source_only") && $0.contains("missing") })
            #expect(diff.migrationSQL.contains { $0.contains("trg_source_only") })
            #expect(diff.differences.contains { $0.contains("trg_target_only") && $0.contains("extra") })
            #expect(diff.dropColumnSQL.contains { $0.contains("trg_target_only") })
        }

        // Clean up
        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(triggerTable) CASCADE", on: conn)
            try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).trg_test_func() CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Sync drops extra trigger on target when allowDropColumns is true")
    func syncDropsExtraTrigger() async throws {
        try? await Self.cleanup()

        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.createSourceTable(on: sourceConn)

        try await Self.createTargetTable(sql: """
            CREATE TABLE \(Self.testSchema).\(Self.tableName) (
                id integer PRIMARY KEY,
                name text NOT NULL,
                description text,
                price numeric(10,2) NOT NULL DEFAULT 0,
                stock_count integer NOT NULL DEFAULT 0,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            )
        """, on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE OR REPLACE FUNCTION \(Self.testSchema).noop_trigger() RETURNS trigger AS $$
            BEGIN RETURN NEW; END;
            $$ LANGUAGE plpgsql
        """, on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TRIGGER trg_extra_trigger
                BEFORE INSERT ON \(Self.testSchema).\(Self.tableName)
                FOR EACH ROW EXECUTE FUNCTION \(Self.testSchema).noop_trigger()
        """, on: targetConn)

        try? await sourceConn.close()
        try? await targetConn.close()

        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: Self.tableName)
        let syncJob = SyncJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [ObjectSpec(id: tableId)],
            dryRun: false,
            allowDropColumns: true,
            force: true
        )
        let orchestrator = SyncOrchestrator(logger: IntegrationTestConfig.logger)
        _ = try await orchestrator.execute(job: syncJob)

        let vc = try await Self.freshTargetConn()
        let postRows = try await vc.query(
            "SELECT tgname FROM pg_trigger WHERE tgrelid = '_mig_test.mig_test_tbl'::regclass AND NOT tgisinternal AND tgname = 'trg_extra_trigger'",
            logger: IntegrationTestConfig.logger
        )
        var postFound = false
        for try await _ in postRows {
            postFound = true
        }
        #expect(!postFound, "Extra trigger should be dropped when allowDropColumns is true")
        try? await vc.close()

        try await Self.cleanup()
        let tc = try await IntegrationTestConfig.connect(to: targetConfig)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).noop_trigger() CASCADE", on: tc)
        try? await tc.close()
    }

    // MARK: - RLS comparison (live sync)

    @Test("Diff detects RLS policy differences on live databases")
    func diffDetectsRLSPolicyDifferences() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        let rlsTableName = "rls_diff_test"
        try await Self.ensureSchema(on: sourceConn)
        try await Self.ensureSchema(on: targetConn)
        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(rlsTableName) CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).\(rlsTableName) (
                    id integer PRIMARY KEY,
                    data text
                )
            """, on: conn)
        }

        try await IntegrationTestConfig.execute("ALTER TABLE \(Self.testSchema).\(rlsTableName) ENABLE ROW LEVEL SECURITY", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE POLICY source_policy ON \(Self.testSchema).\(rlsTableName) FOR SELECT USING (true)
        """, on: sourceConn)

        try await IntegrationTestConfig.execute("ALTER TABLE \(Self.testSchema).\(rlsTableName) ENABLE ROW LEVEL SECURITY", on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE POLICY target_only_policy ON \(Self.testSchema).\(rlsTableName) FOR SELECT USING (id > 0)
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: rlsTableName)
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "There should be RLS policy differences")

        if let diff = objDiff {
            let hasPolicyDiff = diff.differences.contains { $0.contains("RLS policy") }
            #expect(hasPolicyDiff, "Diff should detect RLS policy differences")

            let migSQL = diff.migrationSQL.joined(separator: "\n")
            #expect(migSQL.contains("source_policy"), "Migration SQL should include source RLS policy")

            let dropSQL = diff.dropColumnSQL.joined(separator: "\n")
            #expect(dropSQL.contains("target_only_policy"), "Extra target policy should be in dropColumnSQL")
        }

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).\(rlsTableName) CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    // MARK: - Partitioned table cloning

    @Test("Clone partitioned table introspects partition info correctly")
    func clonePartitionedTableIntrospection() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let eventsId = ObjectIdentifier(type: .table, schema: "public", name: "events")

        let partInfo = try await introspector.partitionInfo(for: eventsId)
        #expect(partInfo != nil, "events table should be partitioned")
        #expect(partInfo?.strategy == "RANGE" || partInfo?.strategy == "range", "events uses RANGE partitioning")

        let children = try await introspector.listPartitions(for: eventsId)
        #expect(children.count >= 2, "events should have at least 2 partitions")
        let childNames = children.map(\.id.name)
        #expect(childNames.contains("events_2025q1"))
        #expect(childNames.contains("events_2025q2"))

        try? await sourceConn.close()
    }

    @Test("Clone partitioned table creates parent and children on target")
    func clonePartitionedTableLive() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.events CASCADE", on: targetConn)

        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.events (
                id integer GENERATED ALWAYS AS IDENTITY,
                event_type text NOT NULL,
                payload jsonb,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            ) PARTITION BY RANGE (created_at)
        """, on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.events_2025q1 PARTITION OF public.events
                FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')
        """, on: targetConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE public.events_2025q2 PARTITION OF public.events
                FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')
        """, on: targetConn)

        let parentRows = try await targetConn.query(
            "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'events'",
            logger: IntegrationTestConfig.logger
        )
        for try await row in parentRows {
            let kind = try row.decode(String.self)
            #expect(kind == "p", "events should be a partitioned table (relkind = 'p')")
        }

        let childRows = try await targetConn.query(
            "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'public.events'::regclass ORDER BY inhrelid::regclass::text",
            logger: IntegrationTestConfig.logger
        )
        var children: [String] = []
        for try await row in childRows {
            children.append(try row.decode(String.self))
        }
        #expect(children.count >= 2, "Partitioned table should have child partitions on target")

        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let eventsId = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let objDiff = try await differ.compareObjects(eventsId, source: sourceIntrospector, target: targetIntrospector)

        if let diff = objDiff {
            let hasColumnDiff = diff.differences.contains { $0.contains("missing in target") || $0.contains("extra in target") }
            #expect(!hasColumnDiff, "Matching partitioned tables should not have column differences")
        }

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public.events CASCADE", on: targetConn)
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Partition child bound specs are introspected correctly")
    func partitionChildBoundSpecs() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let eventsId = ObjectIdentifier(type: .table, schema: "public", name: "events")

        let children = try await introspector.listPartitions(for: eventsId)

        for child in children {
            #expect(!child.boundSpec.isEmpty, "Child \(child.id.name) should have a bound spec")
            #expect(child.boundSpec.contains("FOR VALUES"), "Bound spec should contain FOR VALUES")
        }

        let q1 = children.first { $0.id.name == "events_2025q1" }
        #expect(q1 != nil)
        if let q1Bound = q1 {
            #expect(q1Bound.boundSpec.contains("2025-01-01"), "Q1 should start at 2025-01-01")
            #expect(q1Bound.boundSpec.contains("2025-04-01"), "Q1 should end at 2025-04-01")
        }

        try? await sourceConn.close()
    }
}
