import Foundation
import Logging

/// Generates migration files from a SchemaDiff.
public struct MigrationGenerator: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Generate a migration from a schema diff.
    /// - Parameters:
    ///   - diff: The schema diff to convert
    ///   - description: Human-readable description
    ///   - includeDestructive: Whether to include DROP statements
    /// - Returns: A migration and its SQL content
    public func generate(
        from diff: SchemaDiff,
        description: String,
        includeDestructive: Bool = false
    ) -> (Migration, MigrationSQL) {
        let id = MigrationFileManager.generateId(description: description)

        var upLines: [String] = []
        var downLines: [String] = []
        var objectsAffected: [String] = []
        var irreversible: [String] = []

        // Modified objects: safe ALTER statements
        for objDiff in diff.modified {
            objectsAffected.append(objDiff.id.description)

            if !objDiff.migrationSQL.isEmpty {
                upLines.append("-- Modify \(objDiff.id)")
                upLines.append(contentsOf: objDiff.migrationSQL)
                upLines.append("")
            }

            // Reverse of safe changes
            if !objDiff.reverseMigrationSQL.isEmpty {
                downLines.append("-- Reverse modify \(objDiff.id)")
                downLines.append(contentsOf: objDiff.reverseMigrationSQL)
                downLines.append("")
            }

            // Destructive changes (gated)
            if includeDestructive && !objDiff.dropColumnSQL.isEmpty {
                upLines.append("-- Destructive changes for \(objDiff.id)")
                upLines.append(contentsOf: objDiff.dropColumnSQL)
                upLines.append("")

                if !objDiff.reverseDropColumnSQL.isEmpty {
                    downLines.append("-- Reverse destructive changes for \(objDiff.id)")
                    downLines.append(contentsOf: objDiff.reverseDropColumnSQL)
                    downLines.append("")
                }
            }

            irreversible.append(contentsOf: objDiff.irreversibleChanges)
        }

        // Objects only in source — need CREATE in up, DROP in down
        for id in diff.onlyInSource {
            objectsAffected.append(id.description)
            upLines.append("-- TODO: CREATE \(id) (use 'pg-schema-evo clone' to generate DDL)")
            upLines.append("")
            downLines.append("-- TODO: DROP \(id)")
            downLines.append("")
        }

        // Objects only in target — DROP in up (if destructive), CREATE in down
        if includeDestructive {
            for id in diff.onlyInTarget {
                objectsAffected.append(id.description)
                upLines.append(dropStatementFor(id))
                upLines.append("")
                downLines.append("-- TODO: Re-create \(id)")
                downLines.append("")
            }
        }

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]

        let upSQL = upLines.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines)
        let downSQL = downLines.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines)

        let sql = MigrationSQL(upSQL: upSQL, downSQL: downSQL)

        let migration = Migration(
            id: id,
            description: description,
            generatedAt: formatter.string(from: Date()),
            checksum: "", // Will be set by MigrationFileManager.write()
            objectsAffected: objectsAffected,
            irreversibleChanges: irreversible
        )

        return (migration, sql)
    }

    private func dropStatementFor(_ id: ObjectIdentifier) -> String {
        let keyword: String
        switch id.type {
        case .table: keyword = "TABLE"
        case .view: keyword = "VIEW"
        case .materializedView: keyword = "MATERIALIZED VIEW"
        case .sequence: keyword = "SEQUENCE"
        case .function: keyword = "FUNCTION"
        case .procedure: keyword = "PROCEDURE"
        case .enum, .compositeType: keyword = "TYPE"
        case .schema: keyword = "SCHEMA"
        case .extension: keyword = "EXTENSION"
        default: keyword = "TABLE"
        }
        let name = id.type == .role ? id.name : id.qualifiedName
        return "DROP \(keyword) IF EXISTS \(name) CASCADE;"
    }
}
