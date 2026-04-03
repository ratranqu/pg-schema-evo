import Foundation
import Logging

/// Applies and rolls back migrations against a target database.
public struct MigrationApplicator: Sendable {
    private let config: MigrationConfig
    private let logger: Logger

    public init(config: MigrationConfig = MigrationConfig(), logger: Logger) {
        self.config = config
        self.logger = logger
    }

    /// Apply pending migrations in order.
    /// - Parameters:
    ///   - targetDSN: Target database connection string
    ///   - count: Max number of migrations to apply (nil = all pending)
    ///   - force: If true, apply even if checksum mismatches
    ///   - dryRun: If true, just print the SQL without executing
    /// - Returns: List of applied migration IDs
    public func apply(
        targetDSN: String,
        count: Int? = nil,
        force: Bool = false,
        dryRun: Bool = false
    ) async throws -> [String] {
        let targetConfig = try ConnectionConfig.fromDSN(targetDSN)
        let fileManager = MigrationFileManager(directory: config.directory)
        let store = MigrationStore(config: config, logger: logger)

        // Get all migration files
        let allIds = try fileManager.listMigrations()
        guard !allIds.isEmpty else {
            logger.info("No migration files found in \(config.directory)")
            return []
        }

        // Connect to target and get applied migrations
        let conn = try await PostgresConnectionHelper.connect(config: targetConfig, logger: logger)

        do {
            try await store.ensureTable(on: conn)
            let applied = try await store.listAppliedMigrations(on: conn)
            let appliedIds = Set(applied.map(\.id))

            // Determine pending migrations
            var pending = allIds.filter { !appliedIds.contains($0) }
            if let count {
                pending = Array(pending.prefix(count))
            }

            guard !pending.isEmpty else {
                logger.info("No pending migrations")
                try await conn.close()
                return []
            }

            var appliedResult: [String] = []

            for id in pending {
                let (migration, sql) = try fileManager.read(id: id)

                // Verify checksum
                let checksumValid = try fileManager.verifyChecksum(migration: migration)
                if !checksumValid && !force {
                    try await conn.close()
                    throw PGSchemaEvoError.migrationChecksumMismatch(
                        id: id,
                        expected: migration.checksum,
                        actual: MigrationFileManager.checksum(
                            try String(contentsOfFile: fileManager.sqlPath(for: id), encoding: .utf8)
                        )
                    )
                }

                let fullSQL = sql.fullUpSQL
                if fullSQL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    logger.warning("Migration \(id) has empty UP SQL, skipping")
                    continue
                }

                if dryRun {
                    logger.info("Would apply migration: \(id)")
                    print("-- Migration: \(id)")
                    print("BEGIN;")
                    print(fullSQL)
                    print("COMMIT;")
                    print("")
                } else {
                    logger.info("Applying migration: \(id)")
                    let shell = ShellRunner()
                    let script = "BEGIN;\n\(fullSQL)\nCOMMIT;"
                    let result = try await shell.run(
                        command: "/usr/bin/env",
                        arguments: ["psql"] + targetConfig.psqlArgs() + ["-v", "ON_ERROR_STOP=1"],
                        environment: targetConfig.environment(),
                        input: script
                    )
                    if result.exitCode != 0 {
                        try await conn.close()
                        throw PGSchemaEvoError.shellCommandFailed(
                            command: "psql (migration \(id))",
                            exitCode: result.exitCode,
                            stderr: result.stderr
                        )
                    }
                    try await store.record(migration: migration, on: conn)
                }

                appliedResult.append(id)
            }

            try await conn.close()
            return appliedResult
        } catch {
            try? await conn.close()
            throw error
        }
    }

    /// Rollback the last N applied migrations.
    /// - Parameters:
    ///   - targetDSN: Target database connection string
    ///   - count: Number of migrations to rollback (default 1)
    ///   - force: If true, rollback even if migration has irreversible changes
    ///   - dryRun: If true, just print the SQL without executing
    /// - Returns: List of rolled-back migration IDs
    public func rollback(
        targetDSN: String,
        count: Int = 1,
        force: Bool = false,
        dryRun: Bool = false
    ) async throws -> [String] {
        let targetConfig = try ConnectionConfig.fromDSN(targetDSN)
        let fileManager = MigrationFileManager(directory: config.directory)
        let store = MigrationStore(config: config, logger: logger)

        let conn = try await PostgresConnectionHelper.connect(config: targetConfig, logger: logger)

        do {
            try await store.ensureTable(on: conn)
            let applied = try await store.listAppliedMigrations(on: conn)

            guard !applied.isEmpty else {
                logger.info("No applied migrations to rollback")
                try await conn.close()
                return []
            }

            // Rollback in reverse order
            let toRollback = applied.suffix(count).reversed()
            var rolledBack: [String] = []

            for appliedMigration in toRollback {
                let id = appliedMigration.id

                let (migration, sql) = try fileManager.read(id: id)

                // Check for irreversible changes
                if !migration.irreversibleChanges.isEmpty && !force {
                    try await conn.close()
                    throw PGSchemaEvoError.migrationHasIrreversibleChanges(
                        id: id,
                        changes: migration.irreversibleChanges
                    )
                }

                let fullDownSQL = sql.fullDownSQL
                if fullDownSQL.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    logger.warning("Migration \(id) has empty DOWN SQL, skipping execution")
                    if !dryRun {
                        try await store.remove(id: id, on: conn)
                    }
                    rolledBack.append(id)
                    continue
                }

                if dryRun {
                    logger.info("Would rollback migration: \(id)")
                    print("-- Rollback: \(id)")
                    print("BEGIN;")
                    print(fullDownSQL)
                    print("COMMIT;")
                    print("")
                } else {
                    logger.info("Rolling back migration: \(id)")
                    let shell = ShellRunner()
                    let script = "BEGIN;\n\(fullDownSQL)\nCOMMIT;"
                    let result = try await shell.run(
                        command: "/usr/bin/env",
                        arguments: ["psql"] + targetConfig.psqlArgs() + ["-v", "ON_ERROR_STOP=1"],
                        environment: targetConfig.environment(),
                        input: script
                    )
                    if result.exitCode != 0 {
                        try await conn.close()
                        throw PGSchemaEvoError.shellCommandFailed(
                            command: "psql (rollback \(id))",
                            exitCode: result.exitCode,
                            stderr: result.stderr
                        )
                    }
                    try await store.remove(id: id, on: conn)
                }

                rolledBack.append(id)
            }

            try await conn.close()
            return rolledBack
        } catch {
            try? await conn.close()
            throw error
        }
    }

    /// Get the status of all migrations.
    public func status(targetDSN: String) async throws -> MigrationStatus {
        let targetConfig = try ConnectionConfig.fromDSN(targetDSN)
        let fileManager = MigrationFileManager(directory: config.directory)
        let store = MigrationStore(config: config, logger: logger)

        let allIds = try fileManager.listMigrations()

        let conn = try await PostgresConnectionHelper.connect(config: targetConfig, logger: logger)

        do {
            try await store.ensureTable(on: conn)
            let applied = try await store.listAppliedMigrations(on: conn)
            let appliedMap = Dictionary(uniqueKeysWithValues: applied.map { ($0.id, $0) })

            var entries: [MigrationStatusEntry] = []
            for id in allIds {
                if let app = appliedMap[id] {
                    entries.append(MigrationStatusEntry(
                        id: id,
                        state: .applied,
                        appliedAt: app.appliedAt,
                        appliedBy: app.appliedBy
                    ))
                } else {
                    entries.append(MigrationStatusEntry(id: id, state: .pending))
                }
            }

            // Check for applied migrations not found in filesystem
            for app in applied where !allIds.contains(app.id) {
                entries.append(MigrationStatusEntry(
                    id: app.id,
                    state: .orphaned,
                    appliedAt: app.appliedAt,
                    appliedBy: app.appliedBy
                ))
            }

            try await conn.close()
            return MigrationStatus(entries: entries)
        } catch {
            try? await conn.close()
            throw error
        }
    }
}

/// Status of all migrations.
public struct MigrationStatus: Sendable {
    public let entries: [MigrationStatusEntry]

    public var applied: [MigrationStatusEntry] { entries.filter { $0.state == .applied } }
    public var pending: [MigrationStatusEntry] { entries.filter { $0.state == .pending } }
    public var orphaned: [MigrationStatusEntry] { entries.filter { $0.state == .orphaned } }

    public func render() -> String {
        var lines: [String] = []
        lines.append("Migration Status")
        lines.append(String(repeating: "-", count: 80))

        if entries.isEmpty {
            lines.append("  No migrations found.")
        }

        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"

        for entry in entries {
            let mark: String
            switch entry.state {
            case .applied: mark = "✓"
            case .pending: mark = "○"
            case .orphaned: mark = "!"
            }

            var line = "  \(mark) \(entry.id)"
            if let at = entry.appliedAt {
                line += "  (applied \(formatter.string(from: at)))"
            }
            if entry.state == .orphaned {
                line += "  [ORPHANED - file not found]"
            }
            lines.append(line)
        }

        lines.append(String(repeating: "-", count: 80))
        lines.append("  Applied: \(applied.count)  Pending: \(pending.count)  Orphaned: \(orphaned.count)")

        return lines.joined(separator: "\n")
    }
}

/// Individual migration status entry.
public struct MigrationStatusEntry: Sendable {
    public let id: String
    public let state: MigrationState
    public let appliedAt: Date?
    public let appliedBy: String?

    public init(id: String, state: MigrationState, appliedAt: Date? = nil, appliedBy: String? = nil) {
        self.id = id
        self.state = state
        self.appliedAt = appliedAt
        self.appliedBy = appliedBy
    }
}

/// Possible states for a migration.
public enum MigrationState: String, Sendable {
    case applied
    case pending
    case orphaned
}
