import Testing
import PostgresNIO
import Logging
import Foundation
@testable import PGSchemaEvoCore

@Suite("Migration Tracking Integration Tests", .tags(.integration), .serialized)
struct MigrationTrackingIntegrationTests {

    private static let trackingTable = "_test_mig_tracking"
    private static let trackingSchema = "public"

    private static func makeConfig(dir: String = "/tmp/mig_test") -> MigrationConfig {
        MigrationConfig(
            directory: dir,
            trackingTable: trackingTable,
            trackingSchema: trackingSchema
        )
    }

    private static func cleanup(on conn: PostgresConnection) async throws {
        try await IntegrationTestConfig.execute(
            "DROP TABLE IF EXISTS \(trackingSchema).\(trackingTable) CASCADE", on: conn
        )
    }

    // MARK: - MigrationStore Tests

    @Test("MigrationStore creates tracking table and records migrations")
    func storeBasicOperations() async throws {
        let config = Self.makeConfig()
        let logger = IntegrationTestConfig.logger
        let store = MigrationStore(config: config, logger: logger)

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            // Cleanup
            try await Self.cleanup(on: conn)

            // Create tracking table
            try await store.ensureTable(on: conn)

            // Should have no applied migrations
            let applied = try await store.listAppliedMigrations(on: conn)
            #expect(applied.isEmpty)

            // Record a migration
            let migration = Migration(
                id: "20260403_000001_test_migration",
                description: "Test migration",
                generatedAt: "2026-04-03T00:00:01Z",
                checksum: "abc123"
            )
            try await store.record(migration: migration, on: conn)

            // Should now have one applied migration
            let applied2 = try await store.listAppliedMigrations(on: conn)
            #expect(applied2.count == 1)
            #expect(applied2[0].id == "20260403_000001_test_migration")
            #expect(applied2[0].checksum == "abc123")
            #expect(applied2[0].description == "Test migration")

            // isApplied should return true
            let isApplied = try await store.isApplied(id: "20260403_000001_test_migration", on: conn)
            #expect(isApplied)

            // isApplied for unknown ID should return false
            let isApplied2 = try await store.isApplied(id: "nonexistent", on: conn)
            #expect(!isApplied2)

            // getChecksum should return the stored checksum
            let checksum = try await store.getChecksum(id: "20260403_000001_test_migration", on: conn)
            #expect(checksum == "abc123")

            // getChecksum for unknown ID should return nil
            let checksum2 = try await store.getChecksum(id: "nonexistent", on: conn)
            #expect(checksum2 == nil)

            // Remove the migration
            try await store.remove(id: "20260403_000001_test_migration", on: conn)
            let applied3 = try await store.listAppliedMigrations(on: conn)
            #expect(applied3.isEmpty)

            // Cleanup
            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    @Test("MigrationStore ensureTable is idempotent")
    func ensureTableIdempotent() async throws {
        let config = Self.makeConfig()
        let logger = IntegrationTestConfig.logger
        let store = MigrationStore(config: config, logger: logger)

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)

            // Call ensureTable twice — should not error
            try await store.ensureTable(on: conn)
            try await store.ensureTable(on: conn)

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    @Test("MigrationStore records multiple migrations in order")
    func multipleRecords() async throws {
        let config = Self.makeConfig()
        let logger = IntegrationTestConfig.logger
        let store = MigrationStore(config: config, logger: logger)

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)
            try await store.ensureTable(on: conn)

            // Record 3 migrations
            for i in 1...3 {
                let m = Migration(
                    id: "20260403_00000\(i)_mig_\(i)",
                    description: "Migration \(i)",
                    generatedAt: "2026-04-03T00:00:0\(i)Z",
                    checksum: "hash\(i)"
                )
                try await store.record(migration: m, on: conn)
            }

            let applied = try await store.listAppliedMigrations(on: conn)
            #expect(applied.count == 3)
            // Should be ordered by ID
            #expect(applied[0].id == "20260403_000001_mig_1")
            #expect(applied[1].id == "20260403_000002_mig_2")
            #expect(applied[2].id == "20260403_000003_mig_3")

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    @Test("MigrationStore handles SQL injection in description safely")
    func sqlInjection() async throws {
        let config = Self.makeConfig()
        let logger = IntegrationTestConfig.logger
        let store = MigrationStore(config: config, logger: logger)

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)
            try await store.ensureTable(on: conn)

            let m = Migration(
                id: "20260403_000001_inject",
                description: "O'Reilly; DROP TABLE users;--",
                generatedAt: "2026-04-03T00:00:01Z",
                checksum: "safe"
            )
            try await store.record(migration: m, on: conn)

            let applied = try await store.listAppliedMigrations(on: conn)
            #expect(applied.count == 1)
            #expect(applied[0].description == "O'Reilly; DROP TABLE users;--")

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    // MARK: - Full Migration Workflow

    @Test("Full migration apply and rollback workflow")
    func fullWorkflow() async throws {
        // Setup: Create migration files on disk
        let tmpDir = NSTemporaryDirectory() + "mig_workflow_\(Int(Date().timeIntervalSince1970))"
        let config = Self.makeConfig(dir: tmpDir)
        let logger = IntegrationTestConfig.logger

        defer {
            try? FileManager.default.removeItem(atPath: tmpDir)
        }

        // Create a simple migration file
        let fileManager = MigrationFileManager(directory: tmpDir)
        let migration = Migration(
            id: "20260403_000001_add_test_col",
            description: "Add test column",
            generatedAt: "2026-04-03T00:00:01Z",
            checksum: ""
        )
        let sql = MigrationSQL(
            upSQL: "CREATE TABLE IF NOT EXISTS public._mig_workflow_test (id serial PRIMARY KEY, name text);",
            downSQL: "DROP TABLE IF EXISTS public._mig_workflow_test;"
        )
        try fileManager.write(migration: migration, sql: sql)

        // Connect to target and clean up
        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)
            try await IntegrationTestConfig.execute("DROP TABLE IF EXISTS public._mig_workflow_test CASCADE", on: conn)

            // Apply the migration
            let applicator = MigrationApplicator(config: config, logger: logger)
            let applied = try await applicator.apply(
                targetDSN: IntegrationTestConfig.targetDSN,
                dryRun: false
            )
            #expect(applied.count == 1)
            #expect(applied[0] == "20260403_000001_add_test_col")

            // Verify the table was created
            let rows = try await conn.query(
                PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.tables WHERE table_name = '_mig_workflow_test'"),
                logger: logger
            )
            for try await (count,) in rows.decode((Int,).self, context: .default) {
                #expect(count == 1, "Table should exist after migration apply")
            }

            // Apply again — should be no-op
            let applied2 = try await applicator.apply(
                targetDSN: IntegrationTestConfig.targetDSN,
                dryRun: false
            )
            #expect(applied2.isEmpty)

            // Check status
            let status = try await applicator.status(targetDSN: IntegrationTestConfig.targetDSN)
            #expect(status.applied.count == 1)
            #expect(status.pending.isEmpty)

            // Rollback
            let rolledBack = try await applicator.rollback(
                targetDSN: IntegrationTestConfig.targetDSN,
                count: 1,
                dryRun: false
            )
            #expect(rolledBack.count == 1)

            // Verify the table was dropped
            let rows2 = try await conn.query(
                PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.tables WHERE table_name = '_mig_workflow_test'"),
                logger: logger
            )
            for try await (count,) in rows2.decode((Int,).self, context: .default) {
                #expect(count == 0, "Table should not exist after rollback")
            }

            // Status should show pending again
            let status2 = try await applicator.status(targetDSN: IntegrationTestConfig.targetDSN)
            #expect(status2.pending.count == 1)
            #expect(status2.applied.isEmpty)

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    @Test("Migration dry-run does not execute SQL")
    func dryRun() async throws {
        let tmpDir = NSTemporaryDirectory() + "mig_dryrun_\(Int(Date().timeIntervalSince1970))"
        let config = Self.makeConfig(dir: tmpDir)
        let logger = IntegrationTestConfig.logger

        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fileManager = MigrationFileManager(directory: tmpDir)
        let migration = Migration(
            id: "20260403_000001_dry_run_test",
            description: "Dry run test",
            generatedAt: "2026-04-03T00:00:01Z",
            checksum: ""
        )
        let sql = MigrationSQL(
            upSQL: "CREATE TABLE public._mig_dryrun_tbl (id int);",
            downSQL: "DROP TABLE public._mig_dryrun_tbl;"
        )
        try fileManager.write(migration: migration, sql: sql)

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)

            let applicator = MigrationApplicator(config: config, logger: logger)
            let applied = try await applicator.apply(
                targetDSN: IntegrationTestConfig.targetDSN,
                dryRun: true
            )
            // dry-run reports the migration ID but doesn't execute
            #expect(applied.count == 1)

            // Table should NOT exist
            let rows = try await conn.query(
                PostgresQuery(unsafeSQL: "SELECT count(*) FROM information_schema.tables WHERE table_name = '_mig_dryrun_tbl'"),
                logger: logger
            )
            for try await (count,) in rows.decode((Int,).self, context: .default) {
                #expect(count == 0, "Table should not exist after dry-run")
            }

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    @Test("Migration status detects orphaned migrations")
    func orphanedMigrations() async throws {
        let tmpDir = NSTemporaryDirectory() + "mig_orphan_\(Int(Date().timeIntervalSince1970))"
        let config = Self.makeConfig(dir: tmpDir)
        let logger = IntegrationTestConfig.logger

        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        // Create and apply a migration
        let fileManager = MigrationFileManager(directory: tmpDir)
        let migration = Migration(
            id: "20260403_000001_orphan_test",
            description: "Orphan test",
            generatedAt: "2026-04-03T00:00:01Z",
            checksum: ""
        )
        let sql = MigrationSQL(
            upSQL: "SELECT 1;",
            downSQL: "SELECT 1;"
        )
        try fileManager.write(migration: migration, sql: sql)

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)

            let applicator = MigrationApplicator(config: config, logger: logger)
            _ = try await applicator.apply(targetDSN: IntegrationTestConfig.targetDSN)

            // Now delete the migration file from disk
            try FileManager.default.removeItem(atPath: fileManager.sqlPath(for: migration.id))
            try FileManager.default.removeItem(atPath: fileManager.yamlPath(for: migration.id))

            // Status should show the migration as orphaned
            let status = try await applicator.status(targetDSN: IntegrationTestConfig.targetDSN)
            #expect(status.orphaned.count == 1)
            #expect(status.orphaned[0].id == "20260403_000001_orphan_test")

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }

    @Test("Migration count parameter limits applied migrations")
    func applyWithCount() async throws {
        let tmpDir = NSTemporaryDirectory() + "mig_count_\(Int(Date().timeIntervalSince1970))"
        let config = Self.makeConfig(dir: tmpDir)
        let logger = IntegrationTestConfig.logger

        defer { try? FileManager.default.removeItem(atPath: tmpDir) }

        let fileManager = MigrationFileManager(directory: tmpDir)

        // Create 3 migrations
        for i in 1...3 {
            let m = Migration(
                id: "20260403_00000\(i)_count_test_\(i)",
                description: "Count test \(i)",
                generatedAt: "2026-04-03T00:00:0\(i)Z",
                checksum: ""
            )
            let sql = MigrationSQL(
                upSQL: "SELECT \(i);",
                downSQL: "SELECT \(i);"
            )
            try fileManager.write(migration: m, sql: sql)
        }

        let targetConfig = try IntegrationTestConfig.targetConfig()
        let conn = try await IntegrationTestConfig.connect(to: targetConfig)

        do {
            try await Self.cleanup(on: conn)

            let applicator = MigrationApplicator(config: config, logger: logger)

            // Apply only 2
            let applied = try await applicator.apply(
                targetDSN: IntegrationTestConfig.targetDSN,
                count: 2
            )
            #expect(applied.count == 2)

            // Status should show 2 applied, 1 pending
            let status = try await applicator.status(targetDSN: IntegrationTestConfig.targetDSN)
            #expect(status.applied.count == 2)
            #expect(status.pending.count == 1)

            try await Self.cleanup(on: conn)
            try await conn.close()
        } catch {
            try? await conn.close()
            throw error
        }
    }
}
