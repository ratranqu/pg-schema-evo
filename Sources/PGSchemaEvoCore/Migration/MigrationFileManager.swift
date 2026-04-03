import Foundation
#if canImport(CryptoKit)
import CryptoKit
#endif
import Yams

/// Handles reading and writing migration files (metadata YAML + SQL).
public struct MigrationFileManager: Sendable {
    private let directory: String

    public init(directory: String) {
        self.directory = directory
    }

    /// Generate a migration ID from the current timestamp and description.
    public static func generateId(description: String) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMdd_HHmmss"
        formatter.timeZone = TimeZone(identifier: "UTC")
        let timestamp = formatter.string(from: Date())
        let slug = description
            .lowercased()
            .replacingOccurrences(of: "[^a-z0-9]+", with: "_", options: .regularExpression)
            .trimmingCharacters(in: CharacterSet(charactersIn: "_"))
        let truncated = String(slug.prefix(60))
        return "\(timestamp)_\(truncated)"
    }

    /// Compute SHA-256 checksum of a string.
    public static func checksum(_ content: String) -> String {
        let data = Data(content.utf8)
        #if canImport(CryptoKit)
        let digest = SHA256.hash(data: data)
        return digest.map { String(format: "%02x", $0) }.joined()
        #else
        // Fallback: FNV-1a 64-bit hash (no external crypto dependency needed)
        var hash: UInt64 = 14695981039346656037
        for byte in data {
            hash ^= UInt64(byte)
            hash &*= 1099511628211
        }
        return String(format: "%016llx", hash)
        #endif
    }

    /// Write a migration to disk (both metadata YAML and SQL file).
    public func write(migration: Migration, sql: MigrationSQL) throws {
        let fm = FileManager.default
        if !fm.fileExists(atPath: directory) {
            try fm.createDirectory(atPath: directory, withIntermediateDirectories: true)
        }

        let sqlContent = sql.render(migrationId: migration.id)
        let finalChecksum = Self.checksum(sqlContent)

        var meta = migration
        meta.checksum = finalChecksum

        // Write SQL file
        let sqlPath = self.sqlPath(for: migration.id)
        try sqlContent.write(toFile: sqlPath, atomically: true, encoding: .utf8)

        // Write YAML metadata
        let yamlPath = self.yamlPath(for: migration.id)
        let yamlContent = try serializeMetadata(meta)
        try yamlContent.write(toFile: yamlPath, atomically: true, encoding: .utf8)
    }

    /// Read a migration from disk by ID.
    public func read(id: String) throws -> (Migration, MigrationSQL) {
        let yamlPath = self.yamlPath(for: id)
        let sqlPath = self.sqlPath(for: id)

        guard FileManager.default.fileExists(atPath: yamlPath) else {
            throw PGSchemaEvoError.migrationFileNotFound(path: yamlPath)
        }
        guard FileManager.default.fileExists(atPath: sqlPath) else {
            throw PGSchemaEvoError.migrationFileNotFound(path: sqlPath)
        }

        let yamlContent = try String(contentsOfFile: yamlPath, encoding: .utf8)
        let migration = try deserializeMetadata(yamlContent)

        let sqlContent = try String(contentsOfFile: sqlPath, encoding: .utf8)
        let sql = MigrationSQL.parse(from: sqlContent)

        return (migration, sql)
    }

    /// List all migration IDs found in the directory, sorted by filename (chronological).
    public func listMigrations() throws -> [String] {
        let fm = FileManager.default
        guard fm.fileExists(atPath: directory) else { return [] }

        let files = try fm.contentsOfDirectory(atPath: directory)
        let yamlFiles = files.filter { $0.hasSuffix(".yaml") }
            .sorted()
        return yamlFiles.map { String($0.dropLast(5)) } // strip .yaml
    }

    /// Verify the checksum of a migration SQL file matches the metadata.
    public func verifyChecksum(migration: Migration) throws -> Bool {
        let sqlPath = self.sqlPath(for: migration.id)
        let sqlContent = try String(contentsOfFile: sqlPath, encoding: .utf8)
        let actual = Self.checksum(sqlContent)
        return actual == migration.checksum
    }

    // MARK: - Paths

    public func sqlPath(for id: String) -> String {
        (directory as NSString).appendingPathComponent("\(id).sql")
    }

    public func yamlPath(for id: String) -> String {
        (directory as NSString).appendingPathComponent("\(id).yaml")
    }

    // MARK: - YAML Serialization

    private func serializeMetadata(_ migration: Migration) throws -> String {
        var dict: [String: Any] = [
            "id": migration.id,
            "version": migration.version,
            "description": migration.description,
            "generated_at": migration.generatedAt,
            "checksum": migration.checksum,
        ]
        if !migration.objectsAffected.isEmpty {
            dict["objects_affected"] = migration.objectsAffected
        }
        if !migration.irreversibleChanges.isEmpty {
            dict["irreversible_changes"] = migration.irreversibleChanges
        }
        return try Yams.dump(object: dict, sortKeys: true)
    }

    private func deserializeMetadata(_ yaml: String) throws -> Migration {
        guard let dict = try Yams.load(yaml: yaml) as? [String: Any] else {
            throw PGSchemaEvoError.migrationParseError(path: "", underlying: "Invalid YAML structure")
        }

        guard let id = dict["id"] as? String else {
            throw PGSchemaEvoError.migrationParseError(path: "", underlying: "Missing 'id' field")
        }
        guard let description = dict["description"] as? String else {
            throw PGSchemaEvoError.migrationParseError(path: "", underlying: "Missing 'description' field")
        }

        return Migration(
            id: id,
            description: description,
            generatedAt: dict["generated_at"] as? String ?? "",
            checksum: dict["checksum"] as? String ?? "",
            objectsAffected: dict["objects_affected"] as? [String] ?? [],
            irreversibleChanges: dict["irreversible_changes"] as? [String] ?? [],
            version: dict["version"] as? Int ?? 1
        )
    }
}
