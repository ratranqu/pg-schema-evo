import Testing
import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Extended Integration Tests 2", .tags(.integration), .serialized)
struct ExtendedIntegrationTests2 {

    private static let testSchema = "_ext2_test"

    private static func ensureTestSchema(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute("CREATE SCHEMA IF NOT EXISTS \(testSchema)", on: conn)
    }

    // MARK: - Constraint Diff & ALTER Generation

    @Test("Diff detects added and removed indexes between source and target")
    func diffIndexChanges() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).idx_test CASCADE", on: conn)
            try await IntegrationTestConfig.execute("""
                CREATE TABLE \(Self.testSchema).idx_test (
                    id integer PRIMARY KEY,
                    email text NOT NULL,
                    status text NOT NULL,
                    created_at timestamp DEFAULT now()
                )
            """, on: conn)
        }

        // Source has index on email; target has index on status
        try await IntegrationTestConfig.execute(
            "CREATE INDEX idx_email ON \(Self.testSchema).idx_test (email)", on: sourceConn)
        try await IntegrationTestConfig.execute(
            "CREATE INDEX idx_status ON \(Self.testSchema).idx_test (status)", on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "idx_test")
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Tables with different indexes should show as modified")
        if let diff = objDiff {
            let hasIdxDiff = diff.differences.contains { $0.contains("idx_email") || $0.contains("idx_status") }
            #expect(hasIdxDiff, "Should detect index differences")
        }

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).idx_test CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    @Test("Diff detects constraint differences and generates ALTER SQL")
    func diffConstraintChanges() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).con_test CASCADE", on: conn)
        }

        // Source has a UNIQUE constraint; target does not
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).con_test (
                id integer PRIMARY KEY,
                code text NOT NULL CONSTRAINT uq_code UNIQUE,
                value numeric
            )
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).con_test (
                id integer PRIMARY KEY,
                code text NOT NULL,
                value numeric
            )
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "con_test")
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Tables with different constraints should show as modified")
        if let diff = objDiff {
            let hasConstraintDiff = diff.differences.contains { $0.contains("uq_code") }
            #expect(hasConstraintDiff, "Should detect missing UNIQUE constraint")
            let hasMigrationSQL = diff.migrationSQL.contains { $0.contains("ADD CONSTRAINT") }
            #expect(hasMigrationSQL, "Should generate ALTER TABLE ADD CONSTRAINT")
        }

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).con_test CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    // MARK: - Column Type and Default Diff

    @Test("Diff detects column type changes and generates ALTER")
    func diffColumnTypeChange() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)
        let targetConn = try await IntegrationTestConfig.connect(to: targetConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await Self.ensureTestSchema(on: targetConn)

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).coltype_test CASCADE", on: conn)
        }

        // Source: value is bigint; target: value is integer
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).coltype_test (id integer PRIMARY KEY, value bigint, label text DEFAULT 'new')
        """, on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).coltype_test (id integer PRIMARY KEY, value integer, label text DEFAULT 'old')
        """, on: targetConn)

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: IntegrationTestConfig.logger)

        let differ = SchemaDiffer(logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "coltype_test")
        let objDiff = try await differ.compareObjects(tableId, source: sourceIntrospector, target: targetIntrospector)

        #expect(objDiff != nil, "Tables with column type/default diffs should show as modified")
        if let diff = objDiff {
            let hasTypeDiff = diff.differences.contains { $0.contains("value") && $0.contains("type") }
            #expect(hasTypeDiff, "Should detect column type change")
            let hasDefaultDiff = diff.differences.contains { $0.contains("label") && $0.contains("default") }
            #expect(hasDefaultDiff, "Should detect column default change")
            // Verify ALTER TYPE SQL is generated
            let hasAlterType = diff.migrationSQL.contains { $0.contains("ALTER COLUMN") && $0.contains("TYPE") }
            #expect(hasAlterType, "Should generate ALTER COLUMN TYPE")
        }

        for conn in [sourceConn, targetConn] {
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).coltype_test CASCADE", on: conn)
        }
        try? await sourceConn.close()
        try? await targetConn.close()
    }

    // MARK: - Env Var Interpolation in Config

    @Test("Config loader interpolates environment variables in YAML")
    func configEnvVarInterpolation() async throws {
        let tmpDir = NSTemporaryDirectory()
        let configPath = tmpDir + "test-env-config-\(UUID().uuidString).yaml"

        // Use host/database fields with env var defaults pointing to test DBs
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let yaml = """
        source:
          host: "${TEST_SOURCE_HOST:-\(sourceConfig.host)}"
          port: \(sourceConfig.port)
          database: "\(sourceConfig.database)"
          username: "\(sourceConfig.username)"
          password: "\(sourceConfig.password ?? "")"
        target:
          host: "${TEST_TARGET_HOST:-\(targetConfig.host)}"
          port: \(targetConfig.port)
          database: "\(targetConfig.database)"
          username: "\(targetConfig.username)"
          password: "\(targetConfig.password ?? "")"
        objects:
          - type: table
            schema: public
            name: products
        """
        try yaml.write(toFile: configPath, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: configPath) }

        let loader = ConfigLoader()
        let config = try loader.load(path: configPath)

        // Should resolve env var or use default
        #expect(config.objects.count == 1)
        #expect(config.objects[0].id.name == "products")

        // Verify the parsed config connects to a real database
        let baseJob = config.toCloneJob()
        let cloneJob = CloneJob(
            source: baseJob.source,
            target: baseJob.target,
            objects: baseJob.objects,
            dryRun: true,
            parallel: baseJob.parallel
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: cloneJob)
        #expect(script.contains("products"), "Dry-run should produce script with interpolated config")
    }

    @Test("Config loader throws on undefined env var without default")
    func configUndefinedEnvVar() async throws {
        let tmpDir = NSTemporaryDirectory()
        let configPath = tmpDir + "test-undef-env-\(UUID().uuidString).yaml"
        let yaml = """
        source:
          dsn: "${TOTALLY_UNDEFINED_VAR_XYZ_12345}"
        target:
          dsn: "postgresql://localhost/test"
        objects:
          - type: table
            schema: public
            name: foo
        """
        try yaml.write(toFile: configPath, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(atPath: configPath) }

        let loader = ConfigLoader()
        #expect(throws: PGSchemaEvoError.self) {
            _ = try loader.load(path: configPath)
        }
    }

    // MARK: - PL/pgSQL Function Introspection

    @Test("Introspect PL/pgSQL function preserves body with exception handling")
    func introspectPlpgsqlFunction() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).complex_func(integer, text) CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE OR REPLACE FUNCTION \(Self.testSchema).complex_func(p_id integer, p_name text)
            RETURNS boolean AS $$
            DECLARE
                v_count integer;
            BEGIN
                SELECT count(*) INTO v_count FROM pg_catalog.pg_class WHERE relname = p_name;
                IF v_count > 0 THEN
                    RETURN true;
                ELSE
                    RETURN false;
                END IF;
            EXCEPTION
                WHEN others THEN
                    RETURN false;
            END;
            $$ LANGUAGE plpgsql STABLE
        """, on: sourceConn)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let funcId = ObjectIdentifier(type: .function, schema: Self.testSchema, name: "complex_func", signature: "(integer, text)")
        let metadata = try await introspector.describeFunction(funcId)

        // Verify function body is preserved
        #expect(metadata.definition.contains("DECLARE") || metadata.definition.contains("declare"), "Should preserve DECLARE block")
        #expect(metadata.definition.contains("EXCEPTION") || metadata.definition.contains("exception"), "Should preserve EXCEPTION block")
        #expect(metadata.definition.contains("plpgsql"), "Should preserve language")

        try await IntegrationTestConfig.execute("DROP FUNCTION IF EXISTS \(Self.testSchema).complex_func(integer, text) CASCADE", on: sourceConn)
        try? await sourceConn.close()
    }

    // MARK: - Enum Ordering Preservation

    @Test("Clone preserves enum label ordering")
    func clonePreservesEnumOrder() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).priority_enum CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute(
            "CREATE TYPE \(Self.testSchema).priority_enum AS ENUM ('low', 'medium', 'high', 'critical')", on: sourceConn)

        try? await sourceConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .enum, schema: Self.testSchema, name: "priority_enum")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Verify enum labels appear in correct order in the CREATE TYPE
        #expect(script.contains("priority_enum"), "Script should contain enum name")
        if let createIdx = script.range(of: "'low'"),
           let medIdx = script.range(of: "'medium'"),
           let highIdx = script.range(of: "'high'"),
           let critIdx = script.range(of: "'critical'") {
            #expect(createIdx.lowerBound < medIdx.lowerBound, "low should appear before medium")
            #expect(medIdx.lowerBound < highIdx.lowerBound, "medium should appear before high")
            #expect(highIdx.lowerBound < critIdx.lowerBound, "high should appear before critical")
        } else {
            #expect(Bool(false), "All enum labels should be present in script")
        }

        // Clean up
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).priority_enum CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Large Table Introspection

    @Test("Introspect and generate DDL for table with many columns")
    func introspectWideTable() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).wide_table CASCADE", on: sourceConn)

        // Create a table with 50 columns of various types
        var columns = ["id integer PRIMARY KEY"]
        for i in 1...20 {
            columns.append("text_col_\(i) text")
        }
        for i in 1...10 {
            columns.append("int_col_\(i) integer DEFAULT \(i)")
        }
        for i in 1...10 {
            columns.append("numeric_col_\(i) numeric(10,2)")
        }
        for i in 1...9 {
            columns.append("bool_col_\(i) boolean DEFAULT \(i % 2 == 0 ? "true" : "false")")
        }

        try await IntegrationTestConfig.execute("""
            CREATE TABLE \(Self.testSchema).wide_table (\(columns.joined(separator: ", ")))
        """, on: sourceConn)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let tableId = ObjectIdentifier(type: .table, schema: Self.testSchema, name: "wide_table")
        let metadata = try await introspector.describeTable(tableId)

        // 1 id + 20 text + 10 int + 10 numeric + 9 bool = 50
        #expect(metadata.columns.count == 50, "Should introspect all 50 columns")

        // Verify SQL generation handles all columns
        let sqlGen = TableSQLGenerator()
        let createSQL = try sqlGen.generateCreate(from: metadata)
        #expect(createSQL.contains("text_col_20"), "DDL should include last text column")
        #expect(createSQL.contains("bool_col_9"), "DDL should include last bool column")
        #expect(createSQL.contains("int_col_10"), "DDL should include last int column")

        try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS \(Self.testSchema).wide_table CASCADE", on: sourceConn)
        try? await sourceConn.close()
    }

    // MARK: - Composite Type Introspection & Clone

    @Test("Clone preserves composite type with multiple attributes")
    func cloneCompositeType() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).full_address CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE TYPE \(Self.testSchema).full_address AS (
                street text,
                city text,
                state text,
                zip_code text,
                country text
            )
        """, on: sourceConn)

        try? await sourceConn.close()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(id: ObjectIdentifier(type: .compositeType, schema: Self.testSchema, name: "full_address")),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("full_address"), "Script should contain type name")
        #expect(script.contains("street"), "Script should contain all attributes")
        #expect(script.contains("zip_code"), "Script should contain all attributes")
        #expect(script.contains("country"), "Script should contain all attributes")

        // Clean up
        let sc = try await IntegrationTestConfig.connect(to: sourceConfig)
        try await IntegrationTestConfig.execute("DROP TYPE IF EXISTS \(Self.testSchema).full_address CASCADE", on: sc)
        try? await sc.close()
    }

    // MARK: - Sequence Introspection Details

    @Test("Introspect sequence preserves all parameters")
    func introspectSequenceParams() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let sourceConn = try await IntegrationTestConfig.connect(to: sourceConfig)

        try await Self.ensureTestSchema(on: sourceConn)
        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).custom_seq CASCADE", on: sourceConn)
        try await IntegrationTestConfig.execute("""
            CREATE SEQUENCE \(Self.testSchema).custom_seq START 1000 INCREMENT 5 MINVALUE 100 MAXVALUE 99999 CACHE 10 NO CYCLE
        """, on: sourceConn)

        let introspector = PGCatalogIntrospector(connection: sourceConn, logger: IntegrationTestConfig.logger)
        let seqId = ObjectIdentifier(type: .sequence, schema: Self.testSchema, name: "custom_seq")
        let metadata = try await introspector.describeSequence(seqId)

        #expect(metadata.increment == 5 as Int64, "Should preserve INCREMENT")
        #expect(metadata.minValue == 100 as Int64, "Should preserve MINVALUE")
        #expect(metadata.maxValue == 99999 as Int64, "Should preserve MAXVALUE")
        #expect(metadata.cacheSize == 10 as Int64, "Should preserve CACHE")
        #expect(metadata.isCycled == false, "Should preserve NO CYCLE")

        try await IntegrationTestConfig.execute("DROP SEQUENCE IF EXISTS \(Self.testSchema).custom_seq CASCADE", on: sourceConn)
        try? await sourceConn.close()
    }

}
