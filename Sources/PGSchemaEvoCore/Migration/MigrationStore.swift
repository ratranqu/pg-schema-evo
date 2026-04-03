import PostgresNIO
import Logging
import Foundation

/// Manages the migration tracking table on the target database.
public struct MigrationStore: Sendable {
    private let config: MigrationConfig
    private let logger: Logger

    public init(config: MigrationConfig = MigrationConfig(), logger: Logger) {
        self.config = config
        self.logger = logger
    }

    /// Create the tracking table if it doesn't exist.
    public func ensureTable(on conn: PostgresConnection) async throws {
        let sql = """
            CREATE TABLE IF NOT EXISTS \(config.qualifiedTrackingTable) (
                id TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                applied_by TEXT NOT NULL DEFAULT current_user
            )
            """
        try await conn.query(PostgresQuery(unsafeSQL: sql), logger: logger)
        logger.debug("Migration tracking table ensured: \(config.qualifiedTrackingTable)")
    }

    /// Record a migration as applied.
    public func record(migration: Migration, on conn: PostgresConnection) async throws {
        let sql = """
            INSERT INTO \(config.qualifiedTrackingTable) (id, checksum, description)
            VALUES ('\(escapeSQLString(migration.id))', '\(escapeSQLString(migration.checksum))', '\(escapeSQLString(migration.description))')
            """
        try await conn.query(PostgresQuery(unsafeSQL: sql), logger: logger)
        logger.info("Recorded migration: \(migration.id)")
    }

    /// Remove a migration record (on rollback).
    public func remove(id: String, on conn: PostgresConnection) async throws {
        let sql = "DELETE FROM \(config.qualifiedTrackingTable) WHERE id = '\(escapeSQLString(id))'"
        try await conn.query(PostgresQuery(unsafeSQL: sql), logger: logger)
        logger.info("Removed migration record: \(id)")
    }

    /// List applied migrations ordered by ID (chronological).
    public func listAppliedMigrations(on conn: PostgresConnection) async throws -> [AppliedMigration] {
        let sql = "SELECT id, checksum, description, applied_at, applied_by FROM \(config.qualifiedTrackingTable) ORDER BY id ASC"
        let rows = try await conn.query(PostgresQuery(unsafeSQL: sql), logger: logger)

        var result: [AppliedMigration] = []
        for try await (id, checksum, description, appliedAt, appliedBy) in rows.decode(
            (String, String, String, Date, String).self, context: .default
        ) {
            result.append(AppliedMigration(
                id: id,
                checksum: checksum,
                description: description,
                appliedAt: appliedAt,
                appliedBy: appliedBy
            ))
        }
        return result
    }

    /// Check if a specific migration has been applied.
    public func isApplied(id: String, on conn: PostgresConnection) async throws -> Bool {
        let sql = "SELECT count(*) FROM \(config.qualifiedTrackingTable) WHERE id = '\(escapeSQLString(id))'"
        let rows = try await conn.query(PostgresQuery(unsafeSQL: sql), logger: logger)
        for try await (count,) in rows.decode((Int,).self, context: .default) {
            return count > 0
        }
        return false
    }

    /// Get the stored checksum for a migration.
    public func getChecksum(id: String, on conn: PostgresConnection) async throws -> String? {
        let sql = "SELECT checksum FROM \(config.qualifiedTrackingTable) WHERE id = '\(escapeSQLString(id))'"
        let rows = try await conn.query(PostgresQuery(unsafeSQL: sql), logger: logger)
        for try await (checksum,) in rows.decode((String,).self, context: .default) {
            return checksum
        }
        return nil
    }

    private func escapeSQLString(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}
