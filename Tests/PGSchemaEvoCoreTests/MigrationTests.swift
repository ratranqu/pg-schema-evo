import Testing
import Foundation
import Logging
@testable import PGSchemaEvoCore

// MARK: - Migration Model Tests

@Suite("Migration Model Tests")
struct MigrationModelTests {

    @Test("Migration initializes with all fields")
    func migrationInit() {
        let m = Migration(
            id: "20260403_120000_test",
            description: "Test migration",
            generatedAt: "2026-04-03T12:00:00Z",
            checksum: "abc123",
            objectsAffected: ["table:public.users"],
            irreversibleChanges: ["Cannot remove enum value"]
        )
        #expect(m.id == "20260403_120000_test")
        #expect(m.description == "Test migration")
        #expect(m.checksum == "abc123")
        #expect(m.objectsAffected.count == 1)
        #expect(m.irreversibleChanges.count == 1)
        #expect(m.version == 1)
    }

    @Test("Migration defaults version to 1")
    func migrationDefaultVersion() {
        let m = Migration(id: "x", description: "d", generatedAt: "", checksum: "")
        #expect(m.version == 1)
        #expect(m.objectsAffected.isEmpty)
        #expect(m.irreversibleChanges.isEmpty)
    }
}

// MARK: - MigrationSQL Tests

@Suite("MigrationSQL Tests")
struct MigrationSQLTests {

    @Test("MigrationSQL renders and parses round-trip")
    func renderAndParse() {
        let sql = MigrationSQL(
            upSQL: "ALTER TABLE users ADD COLUMN age integer;",
            downSQL: "ALTER TABLE users DROP COLUMN age;",
            customUpSQL: "INSERT INTO audit VALUES ('added age');",
            customDownSQL: "DELETE FROM audit WHERE msg = 'added age';",
            dataUpSQL: "UPDATE users SET age = 0;",
            dataDownSQL: "-- no data rollback needed"
        )

        let rendered = sql.render(migrationId: "test_001")
        #expect(rendered.contains("-- === UP ==="))
        #expect(rendered.contains("-- === DOWN ==="))
        #expect(rendered.contains("-- === CUSTOM UP ==="))
        #expect(rendered.contains("-- === CUSTOM DOWN ==="))
        #expect(rendered.contains("-- === DATA UP ==="))
        #expect(rendered.contains("-- === DATA DOWN ==="))
        #expect(rendered.contains("ALTER TABLE users ADD COLUMN age integer;"))

        // Parse it back
        let parsed = MigrationSQL.parse(from: rendered)
        #expect(parsed.upSQL == sql.upSQL)
        #expect(parsed.downSQL == sql.downSQL)
        #expect(parsed.customUpSQL == sql.customUpSQL)
        #expect(parsed.customDownSQL == sql.customDownSQL)
        #expect(parsed.dataUpSQL == sql.dataUpSQL)
        #expect(parsed.dataDownSQL == sql.dataDownSQL)
    }

    @Test("MigrationSQL fullUpSQL combines sections in order")
    func fullUpSQL() {
        let sql = MigrationSQL(
            upSQL: "ALTER TABLE a;",
            downSQL: "DROP a;",
            customUpSQL: "INSERT INTO b;",
            dataUpSQL: "COPY c;"
        )
        let full = sql.fullUpSQL
        #expect(full.contains("ALTER TABLE a;"))
        #expect(full.contains("INSERT INTO b;"))
        #expect(full.contains("COPY c;"))
        // up comes before custom which comes before data
        let upIdx = full.range(of: "ALTER TABLE a;")!.lowerBound
        let customIdx = full.range(of: "INSERT INTO b;")!.lowerBound
        let dataIdx = full.range(of: "COPY c;")!.lowerBound
        #expect(upIdx < customIdx)
        #expect(customIdx < dataIdx)
    }

    @Test("MigrationSQL fullDownSQL combines sections in reverse order")
    func fullDownSQL() {
        let sql = MigrationSQL(
            upSQL: "SCHEMA_UP;",
            downSQL: "SCHEMA_DOWN;",
            customDownSQL: "CUSTOM_DOWN;",
            dataDownSQL: "DATA_DOWN;"
        )
        let full = sql.fullDownSQL
        #expect(full.contains("SCHEMA_DOWN;"))
        #expect(full.contains("CUSTOM_DOWN;"))
        #expect(full.contains("DATA_DOWN;"))
        // data down comes before custom down which comes before schema down
        let dataIdx = full.range(of: "DATA_DOWN;")!.lowerBound
        let customIdx = full.range(of: "CUSTOM_DOWN;")!.lowerBound
        let downIdx = full.range(of: "SCHEMA_DOWN;")!.lowerBound
        #expect(dataIdx < customIdx)
        #expect(customIdx < downIdx)
    }

    @Test("MigrationSQL parse handles empty content")
    func parseEmpty() {
        let parsed = MigrationSQL.parse(from: "")
        #expect(parsed.upSQL.isEmpty)
        #expect(parsed.downSQL.isEmpty)
    }

    @Test("MigrationSQL parse handles minimal sections")
    func parseMinimal() {
        let content = """
        -- === UP ===
        SELECT 1;
        -- === DOWN ===
        SELECT 2;
        """
        let parsed = MigrationSQL.parse(from: content)
        #expect(parsed.upSQL == "SELECT 1;")
        #expect(parsed.downSQL == "SELECT 2;")
        #expect(parsed.customUpSQL.isEmpty)
        #expect(parsed.customDownSQL.isEmpty)
    }

    @Test("MigrationSQL skips empty sections in fullUpSQL")
    func emptyCustomSections() {
        let sql = MigrationSQL(upSQL: "ALTER TABLE a;", downSQL: "DROP a;")
        let full = sql.fullUpSQL
        #expect(full == "ALTER TABLE a;")
    }

    @Test("MigrationSQL render includes migration id header")
    func renderHeader() {
        let sql = MigrationSQL(upSQL: "UP;", downSQL: "DOWN;")
        let rendered = sql.render(migrationId: "my_migration")
        #expect(rendered.contains("-- Migration: my_migration"))
        #expect(rendered.contains("-- Generated by pg-schema-evo"))
    }
}

// MARK: - MigrationFileManager Tests

@Suite("MigrationFileManager Tests")
struct MigrationFileManagerTests {

    @Test("generateId creates timestamp-based ID")
    func generateId() {
        let id = MigrationFileManager.generateId(description: "Add Users Table")
        // Should match pattern: YYYYMMDD_HHMMSS_slug
        #expect(id.contains("_add_users_table"))
        let parts = id.split(separator: "_", maxSplits: 2)
        #expect(parts.count == 3) // date, time, slug... actually it splits more
        // Just verify it starts with a date-like pattern
        #expect(id.first?.isNumber == true)
    }

    @Test("generateId truncates long descriptions")
    func generateIdTruncation() {
        let longDesc = String(repeating: "abcdefghij ", count: 20) // 220 chars
        let id = MigrationFileManager.generateId(description: longDesc)
        // Slug part should be max 60 chars
        let underscoreCount = id.filter { $0 == "_" }.count
        #expect(underscoreCount >= 2) // at least date_time_slug
        #expect(id.count <= 80) // reasonable total length
    }

    @Test("generateId handles special characters")
    func generateIdSpecialChars() {
        let id = MigrationFileManager.generateId(description: "Fix: user's table (DROP!)")
        #expect(id.contains("fix"))
        #expect(id.contains("user"))
        #expect(!id.contains("'"))
        #expect(!id.contains("!"))
        #expect(!id.contains("("))
    }

    @Test("checksum produces consistent results")
    func checksumConsistency() {
        let content = "ALTER TABLE users ADD COLUMN age integer;"
        let c1 = MigrationFileManager.checksum(content)
        let c2 = MigrationFileManager.checksum(content)
        #expect(c1 == c2)
        #expect(!c1.isEmpty)
    }

    @Test("checksum differs for different content")
    func checksumDifference() {
        let c1 = MigrationFileManager.checksum("SELECT 1;")
        let c2 = MigrationFileManager.checksum("SELECT 2;")
        #expect(c1 != c2)
    }

    @Test("write and read round-trip")
    func writeAndRead() throws {
        let tmpDir = NSTemporaryDirectory() + "migration_test_\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fm = MigrationFileManager(directory: tmpDir)

        let migration = Migration(
            id: "20260403_120000_test_migration",
            description: "Test migration",
            generatedAt: "2026-04-03T12:00:00Z",
            checksum: "",
            objectsAffected: ["table:public.users"],
            irreversibleChanges: []
        )
        let sql = MigrationSQL(
            upSQL: "ALTER TABLE users ADD COLUMN age integer;",
            downSQL: "ALTER TABLE users DROP COLUMN age;"
        )

        try fm.write(migration: migration, sql: sql)

        // Read back
        let (readMigration, readSQL) = try fm.read(id: migration.id)
        #expect(readMigration.id == migration.id)
        #expect(readMigration.description == migration.description)
        #expect(!readMigration.checksum.isEmpty)
        #expect(readSQL.upSQL == sql.upSQL)
        #expect(readSQL.downSQL == sql.downSQL)
    }

    @Test("listMigrations returns sorted IDs")
    func listMigrations() throws {
        let tmpDir = NSTemporaryDirectory() + "migration_list_\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fm = MigrationFileManager(directory: tmpDir)

        let ids = ["20260401_000000_first", "20260403_000000_third", "20260402_000000_second"]
        for id in ids {
            let m = Migration(id: id, description: id, generatedAt: "", checksum: "")
            let s = MigrationSQL(upSQL: "SELECT 1;", downSQL: "SELECT 2;")
            try fm.write(migration: m, sql: s)
        }

        let listed = try fm.listMigrations()
        #expect(listed.count == 3)
        #expect(listed[0] == "20260401_000000_first")
        #expect(listed[1] == "20260402_000000_second")
        #expect(listed[2] == "20260403_000000_third")
    }

    @Test("listMigrations returns empty for non-existent directory")
    func listMigrationsEmpty() throws {
        let fm = MigrationFileManager(directory: "/tmp/nonexistent_\(UUID().uuidString)")
        let listed = try fm.listMigrations()
        #expect(listed.isEmpty)
    }

    @Test("verifyChecksum detects tampering")
    func verifyChecksum() throws {
        let tmpDir = NSTemporaryDirectory() + "migration_checksum_\(UUID().uuidString)"
        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fm = MigrationFileManager(directory: tmpDir)
        let id = "20260403_120000_checksum_test"
        let m = Migration(id: id, description: "test", generatedAt: "", checksum: "")
        let s = MigrationSQL(upSQL: "SELECT 1;", downSQL: "SELECT 2;")
        try fm.write(migration: m, sql: s)

        // Read back — checksum should match
        let (readM, _) = try fm.read(id: id)
        #expect(try fm.verifyChecksum(migration: readM))

        // Tamper with the SQL file
        let sqlPath = fm.sqlPath(for: id)
        try "TAMPERED".write(toFile: sqlPath, atomically: true, encoding: .utf8)
        #expect(try !fm.verifyChecksum(migration: readM))
    }

    @Test("read throws for missing files")
    func readMissing() throws {
        let fm = MigrationFileManager(directory: "/tmp/nonexistent_\(UUID().uuidString)")
        #expect(throws: PGSchemaEvoError.self) {
            _ = try fm.read(id: "nonexistent")
        }
    }
}

// MARK: - MigrationConfig Tests

@Suite("MigrationConfig Tests")
struct MigrationConfigTests {

    @Test("MigrationConfig has sensible defaults")
    func defaults() {
        let c = MigrationConfig()
        #expect(c.directory == "migrations")
        #expect(c.trackingTable == "_pg_schema_evo_migrations")
        #expect(c.trackingSchema == "public")
        #expect(c.qualifiedTrackingTable == "public._pg_schema_evo_migrations")
    }

    @Test("MigrationConfig allows customization")
    func custom() {
        let c = MigrationConfig(
            directory: "/opt/migrations",
            trackingTable: "my_migrations",
            trackingSchema: "app"
        )
        #expect(c.directory == "/opt/migrations")
        #expect(c.qualifiedTrackingTable == "app.my_migrations")
    }
}

// MARK: - MigrationGenerator Tests

@Suite("MigrationGenerator Tests")
struct MigrationGeneratorTests {

    @Test("Generate migration from empty diff produces empty SQL")
    func emptyDiff() {
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [], matching: 5)
        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)
        let (migration, sql) = gen.generate(from: diff, description: "empty diff")

        #expect(migration.id.contains("empty_diff"))
        #expect(sql.upSQL.isEmpty)
        #expect(sql.downSQL.isEmpty)
        #expect(migration.objectsAffected.isEmpty)
    }

    @Test("Generate migration from modified objects produces up and down SQL")
    func modifiedObjects() {
        let objDiff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            differences: ["Column age: missing in target"],
            migrationSQL: ["ALTER TABLE \"public\".\"users\" ADD COLUMN \"age\" integer;"],
            reverseMigrationSQL: ["ALTER TABLE \"public\".\"users\" DROP COLUMN \"age\";"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)

        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)
        let (migration, sql) = gen.generate(from: diff, description: "add age column")

        #expect(sql.upSQL.contains("ADD COLUMN"))
        #expect(sql.downSQL.contains("DROP COLUMN"))
        #expect(migration.objectsAffected.count == 1)
    }

    @Test("Generate migration includes irreversible changes")
    func irreversibleChanges() {
        let objDiff = ObjectDiff(
            id: ObjectIdentifier(type: .enum, schema: "public", name: "status"),
            differences: ["Label 'archived': missing in target"],
            migrationSQL: ["ALTER TYPE \"public\".\"status\" ADD VALUE 'archived';"],
            irreversibleChanges: ["Cannot remove enum value 'archived' from \"public\".\"status\" (PostgreSQL limitation)"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)

        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)
        let (migration, _) = gen.generate(from: diff, description: "add enum value")

        #expect(migration.irreversibleChanges.count == 1)
        #expect(migration.irreversibleChanges[0].contains("Cannot remove enum value"))
    }

    @Test("Generate migration with destructive changes includes drops")
    func destructiveChanges() {
        let objDiff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            differences: ["Column temp: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" DROP COLUMN \"temp\";"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" ADD COLUMN \"temp\" text;"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)

        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)

        // Without destructive flag
        let (_, sqlSafe) = gen.generate(from: diff, description: "test", includeDestructive: false)
        #expect(!sqlSafe.upSQL.contains("DROP COLUMN"))

        // With destructive flag
        let (_, sqlDestructive) = gen.generate(from: diff, description: "test", includeDestructive: true)
        #expect(sqlDestructive.upSQL.contains("DROP COLUMN"))
        #expect(sqlDestructive.downSQL.contains("ADD COLUMN"))
    }

    @Test("Generate migration handles only-in-source objects")
    func onlyInSource() {
        let diff = SchemaDiff(
            onlyInSource: [ObjectIdentifier(type: .table, schema: "public", name: "new_table")],
            onlyInTarget: [],
            modified: [],
            matching: 0
        )
        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)
        let (migration, sql) = gen.generate(from: diff, description: "new table")

        #expect(sql.upSQL.contains("TODO: CREATE"))
        #expect(sql.downSQL.contains("TODO: DROP"))
        #expect(migration.objectsAffected.count == 1)
    }

    @Test("Generate migration handles only-in-target with destructive flag")
    func onlyInTargetDestructive() {
        let types: [(ObjectType, String)] = [
            (.table, "TABLE"),
            (.view, "VIEW"),
            (.materializedView, "MATERIALIZED VIEW"),
            (.sequence, "SEQUENCE"),
            (.function, "FUNCTION"),
            (.procedure, "PROCEDURE"),
            (.enum, "TYPE"),
            (.compositeType, "TYPE"),
            (.schema, "SCHEMA"),
            (.extension, "EXTENSION"),
        ]
        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)

        for (objType, keyword) in types {
            let diff = SchemaDiff(
                onlyInSource: [],
                onlyInTarget: [ObjectIdentifier(type: objType, schema: "public", name: "obj")],
                modified: [],
                matching: 0
            )
            let (migration, sql) = gen.generate(from: diff, description: "drop \(keyword)", includeDestructive: true)
            #expect(sql.upSQL.contains("DROP \(keyword) IF EXISTS"), "Expected DROP \(keyword) for type \(objType)")
            #expect(sql.upSQL.contains("CASCADE"))
            #expect(sql.downSQL.contains("TODO: Re-create"))
            #expect(migration.objectsAffected.count == 1)
        }
    }

    @Test("Generate migration skips only-in-target without destructive flag")
    func onlyInTargetNotDestructive() {
        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [ObjectIdentifier(type: .table, schema: "public", name: "old_table")],
            modified: [],
            matching: 0
        )
        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)
        let (migration, sql) = gen.generate(from: diff, description: "no drop")

        #expect(sql.upSQL.isEmpty)
        #expect(sql.downSQL.isEmpty)
        #expect(migration.objectsAffected.isEmpty)
    }

    @Test("Generate migration for role uses unqualified name in DROP")
    func roleDropUsesName() {
        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [ObjectIdentifier(type: .role, schema: nil, name: "app_user")],
            modified: [],
            matching: 0
        )
        let logger = Logger(label: "test")
        let gen = MigrationGenerator(logger: logger)
        let (_, sql) = gen.generate(from: diff, description: "drop role", includeDestructive: true)

        #expect(sql.upSQL.contains("app_user"))
    }

    @Test("MigrationSQL render includes all custom sections")
    func renderCustomSections() {
        let sql = MigrationSQL(
            upSQL: "CREATE TABLE t(id int);",
            downSQL: "DROP TABLE t;",
            customUpSQL: "INSERT INTO t VALUES(1);",
            customDownSQL: "DELETE FROM t;",
            dataUpSQL: "COPY t FROM stdin;",
            dataDownSQL: "TRUNCATE t;"
        )
        let rendered = sql.render(migrationId: "test_001")
        #expect(rendered.contains("-- === UP ==="))
        #expect(rendered.contains("-- === DOWN ==="))
        #expect(rendered.contains("-- === CUSTOM UP ==="))
        #expect(rendered.contains("-- === CUSTOM DOWN ==="))
        #expect(rendered.contains("-- === DATA UP ==="))
        #expect(rendered.contains("-- === DATA DOWN ==="))
        #expect(rendered.contains("CREATE TABLE t(id int);"))
        #expect(rendered.contains("COPY t FROM stdin;"))
    }

    @Test("MigrationSQL parse round-trips all sections")
    func parseAllSections() {
        let original = MigrationSQL(
            upSQL: "ALTER TABLE t ADD COLUMN c int;",
            downSQL: "ALTER TABLE t DROP COLUMN c;",
            customUpSQL: "UPDATE t SET c = 0;",
            customDownSQL: "-- no custom down needed",
            dataUpSQL: "INSERT INTO t SELECT * FROM backup;",
            dataDownSQL: "DELETE FROM t WHERE c IS NOT NULL;"
        )
        let rendered = original.render(migrationId: "roundtrip_test")
        let parsed = MigrationSQL.parse(from: rendered)
        #expect(parsed.upSQL == original.upSQL)
        #expect(parsed.downSQL == original.downSQL)
        #expect(parsed.customUpSQL == original.customUpSQL)
        #expect(parsed.customDownSQL == original.customDownSQL)
        #expect(parsed.dataUpSQL == original.dataUpSQL)
        #expect(parsed.dataDownSQL == original.dataDownSQL)
    }

    @Test("MigrationFileManager checksum is deterministic")
    func checksumDeterministic() {
        let content = "SELECT 1; SELECT 2;"
        let c1 = MigrationFileManager.checksum(content)
        let c2 = MigrationFileManager.checksum(content)
        #expect(c1 == c2)
        #expect(!c1.isEmpty)
    }

    @Test("MigrationFileManager checksum differs for different content")
    func checksumDiffers() {
        let c1 = MigrationFileManager.checksum("SELECT 1;")
        let c2 = MigrationFileManager.checksum("SELECT 2;")
        #expect(c1 != c2)
    }

    @Test("MigrationFileManager generateId contains description slug")
    func generateIdSlug() {
        let id = MigrationFileManager.generateId(description: "Add Users Table")
        #expect(id.contains("add_users_table"))
        #expect(id.count <= 80)
    }

    @Test("MigrationStatus render shows applied by user")
    func renderAppliedBy() {
        let entries = [
            MigrationStatusEntry(id: "m1", state: .applied, appliedAt: Date(), appliedBy: "admin"),
        ]
        let status = MigrationStatus(entries: entries)
        let output = status.render()
        #expect(output.contains("m1"))
        #expect(output.contains("Applied: 1"))
    }
}

// MARK: - MigrationStatus Tests

@Suite("MigrationStatus Tests")
struct MigrationStatusTests {

    @Test("MigrationStatus categorizes entries")
    func categorize() {
        let entries = [
            MigrationStatusEntry(id: "001", state: .applied, appliedAt: Date()),
            MigrationStatusEntry(id: "002", state: .pending),
            MigrationStatusEntry(id: "003", state: .applied, appliedAt: Date()),
            MigrationStatusEntry(id: "orphan", state: .orphaned, appliedAt: Date()),
        ]
        let status = MigrationStatus(entries: entries)
        #expect(status.applied.count == 2)
        #expect(status.pending.count == 1)
        #expect(status.orphaned.count == 1)
    }

    @Test("MigrationStatus render includes all entries")
    func render() {
        let entries = [
            MigrationStatusEntry(id: "20260401_test", state: .applied, appliedAt: Date()),
            MigrationStatusEntry(id: "20260402_test", state: .pending),
            MigrationStatusEntry(id: "20260400_orphan", state: .orphaned, appliedAt: Date()),
        ]
        let status = MigrationStatus(entries: entries)
        let output = status.render()

        #expect(output.contains("20260401_test"))
        #expect(output.contains("20260402_test"))
        #expect(output.contains("20260400_orphan"))
        #expect(output.contains("ORPHANED"))
        #expect(output.contains("Applied: 1"))
        #expect(output.contains("Pending: 1"))
        #expect(output.contains("Orphaned: 1"))
    }

    @Test("MigrationStatus render handles empty")
    func renderEmpty() {
        let status = MigrationStatus(entries: [])
        let output = status.render()
        #expect(output.contains("No migrations found"))
    }
}

// MARK: - AppliedMigration Tests

@Suite("AppliedMigration Tests")
struct AppliedMigrationTests {

    @Test("AppliedMigration stores all fields")
    func init_test() {
        let now = Date()
        let am = AppliedMigration(
            id: "test",
            checksum: "abc",
            description: "desc",
            appliedAt: now,
            appliedBy: "user"
        )
        #expect(am.id == "test")
        #expect(am.checksum == "abc")
        #expect(am.description == "desc")
        #expect(am.appliedAt == now)
        #expect(am.appliedBy == "user")
    }
}

// MARK: - Error Tests

@Suite("Migration Error Tests")
struct MigrationErrorTests {

    @Test("Migration errors have descriptive messages")
    func errorMessages() {
        let errors: [PGSchemaEvoError] = [
            .migrationFileNotFound(path: "/tmp/test.sql"),
            .migrationParseError(path: "/tmp/test.yaml", underlying: "bad format"),
            .migrationAlreadyApplied(id: "001"),
            .migrationNotApplied(id: "001"),
            .migrationChecksumMismatch(id: "001", expected: "aaaaaa", actual: "bbbbbb"),
            .migrationHasIrreversibleChanges(id: "001", changes: ["Cannot remove enum"]),
            .migrationDirectoryNotFound(path: "/opt/migrations"),
        ]

        for error in errors {
            let desc = error.errorDescription ?? ""
            #expect(!desc.isEmpty, "Error should have a description: \(error)")
        }
    }

    @Test("Checksum mismatch error truncates hashes")
    func checksumErrorTruncation() {
        let error = PGSchemaEvoError.migrationChecksumMismatch(
            id: "test",
            expected: "abcdef123456789",
            actual: "zyxwvu987654321"
        )
        let desc = error.errorDescription ?? ""
        #expect(desc.contains("abcdef123456"))
        #expect(desc.contains("zyxwvu987654"))
    }
}
