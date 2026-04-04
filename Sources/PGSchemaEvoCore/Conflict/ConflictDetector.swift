import Foundation
import Logging

/// Transforms a `SchemaDiff` into a structured `ConflictReport` by classifying
/// destructive and irreversible differences as `SchemaConflict` entries.
///
/// Safe forward-only migrations (ADD COLUMN, ALTER TYPE, etc.) are NOT conflicts —
/// they are always applied. Only destructive changes (DROP COLUMN, DROP CONSTRAINT,
/// objects only in target) and irreversible changes need conflict resolution.
public struct ConflictDetector: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Detect conflicts from a pre-computed schema diff.
    /// Only destructive/irreversible changes are classified as conflicts.
    /// Safe migration SQL is NOT included — it is always applied by the orchestrator.
    public func detect(from diff: SchemaDiff) -> ConflictReport {
        var conflicts: [SchemaConflict] = []

        // Objects only in target — would be dropped if syncing source→target
        for id in diff.onlyInTarget {
            conflicts.append(SchemaConflict(
                objectIdentifier: id.description,
                kind: .objectOnlyInTarget,
                description: "\(id) exists only in target and would be dropped",
                sourceSQL: [dropStatementFor(id)],
                isDestructive: true,
                detail: "Object type: \(id.type.rawValue)"
            ))
        }

        // Modified objects — only classify destructive/irreversible changes
        for objDiff in diff.modified {
            conflicts.append(contentsOf: classifyObjectDiff(objDiff))
        }

        logger.debug("Conflict detection complete: \(conflicts.count) conflict(s) found")
        return ConflictReport(conflicts: conflicts)
    }

    /// Classify destructive and irreversible differences within a single ObjectDiff.
    /// Safe migration changes (migrationSQL) are NOT conflicts.
    private func classifyObjectDiff(_ objDiff: ObjectDiff) -> [SchemaConflict] {
        var conflicts: [SchemaConflict] = []
        let objectId = objDiff.id.description

        // Destructive changes (drop columns, constraints, indexes, triggers, policies)
        let destructiveDiffs = objDiff.differences.filter { isExtraInTargetDiff($0) }
        if !destructiveDiffs.isEmpty && !objDiff.dropColumnSQL.isEmpty {
            for (index, diff) in destructiveDiffs.enumerated() {
                let sql = index < objDiff.dropColumnSQL.count ? [objDiff.dropColumnSQL[index]] : []
                let reverseSQL = index < objDiff.reverseDropColumnSQL.count ? [objDiff.reverseDropColumnSQL[index]] : []
                conflicts.append(SchemaConflict(
                    objectIdentifier: objectId,
                    kind: .extraInTarget,
                    description: diff,
                    sourceSQL: sql,
                    targetSQL: reverseSQL,
                    isDestructive: true,
                    detail: "Dropping this may cause data loss"
                ))
            }
        }

        // Irreversible changes
        for change in objDiff.irreversibleChanges {
            conflicts.append(SchemaConflict(
                objectIdentifier: objectId,
                kind: .irreversibleChange,
                description: change,
                sourceSQL: [],
                isDestructive: false,
                isIrreversible: true,
                detail: "This change cannot be undone"
            ))
        }

        return conflicts
    }

    /// Check if a difference string indicates something extra in target.
    private func isExtraInTargetDiff(_ diff: String) -> Bool {
        diff.contains("extra in target") ||
        (diff.contains("enabled on target but not on source")) ||
        (diff.contains("forced on target but not on source"))
    }

    /// Generate a DROP statement for an object identifier.
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
