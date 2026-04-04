import Foundation

/// Aggregates all detected conflicts with rendering and filtering capabilities.
public struct ConflictReport: Sendable, Codable, Equatable {
    /// All detected conflicts.
    public let conflicts: [SchemaConflict]
    /// When detection was performed.
    public let detectedAt: Date

    public init(conflicts: [SchemaConflict], detectedAt: Date = Date()) {
        self.conflicts = conflicts
        self.detectedAt = detectedAt
    }

    /// Whether there are no conflicts.
    public var isEmpty: Bool { conflicts.isEmpty }

    /// Total number of conflicts.
    public var count: Int { conflicts.count }

    /// Conflicts that would cause data loss.
    public var destructiveConflicts: [SchemaConflict] {
        conflicts.filter(\.isDestructive)
    }

    /// Conflicts that are safe to apply (no data loss).
    public var nonDestructiveConflicts: [SchemaConflict] {
        conflicts.filter { !$0.isDestructive }
    }

    /// Conflicts that cannot be reversed.
    public var irreversibleConflicts: [SchemaConflict] {
        conflicts.filter(\.isIrreversible)
    }

    /// Render as a human-readable text summary.
    public func renderText() -> String {
        guard !isEmpty else {
            return "No conflicts detected."
        }

        var lines: [String] = []
        lines.append("Conflicts detected: \(count) (\(destructiveConflicts.count) destructive)")
        lines.append("")

        for (index, conflict) in conflicts.enumerated() {
            let marker: String
            if conflict.isDestructive {
                marker = "⚠ DESTRUCTIVE"
            } else if conflict.isIrreversible {
                marker = "⚠ IRREVERSIBLE"
            } else {
                marker = "~"
            }
            lines.append("  [\(index + 1)/\(count)] \(conflict.objectIdentifier) [\(marker)]")
            lines.append("    Kind: \(conflict.kind.rawValue)")
            lines.append("    \(conflict.description)")
            if let detail = conflict.detail {
                lines.append("    Detail: \(detail)")
            }
            if !conflict.sourceSQL.isEmpty {
                lines.append("    Source SQL:")
                for sql in conflict.sourceSQL {
                    lines.append("      \(sql)")
                }
            }
            if !conflict.targetSQL.isEmpty {
                lines.append("    Target SQL:")
                for sql in conflict.targetSQL {
                    lines.append("      \(sql)")
                }
            }
            lines.append("")
        }

        return lines.joined(separator: "\n")
    }
}
