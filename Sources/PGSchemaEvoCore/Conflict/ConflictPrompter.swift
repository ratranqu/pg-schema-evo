import Foundation

/// Protocol for prompting the user to resolve individual conflicts.
public protocol ConflictPrompter: Sendable {
    func prompt(conflict: SchemaConflict, index: Int, total: Int) async -> ResolutionChoice
}

/// Terminal-based conflict prompter that reads from stdin.
public struct TerminalConflictPrompter: ConflictPrompter, Sendable {
    /// When true, auto-accept non-destructive conflicts as applySource.
    public let autoAccept: Bool

    public init(autoAccept: Bool = false) {
        self.autoAccept = autoAccept
    }

    public func prompt(conflict: SchemaConflict, index: Int, total: Int) async -> ResolutionChoice {
        let marker = conflict.isDestructive ? "\u{1B}[31m⚠ DESTRUCTIVE\u{1B}[0m" :
                     conflict.isIrreversible ? "\u{1B}[33m⚠ IRREVERSIBLE\u{1B}[0m" : ""

        var output = "\nCONFLICT [\(index)/\(total)] \(conflict.objectIdentifier)"
        if !marker.isEmpty {
            output += " \(marker)"
        }
        output += "\n  \(conflict.description)"
        if let detail = conflict.detail {
            output += "\n  Detail: \(detail)"
        }
        if !conflict.sourceSQL.isEmpty {
            output += "\n  Source action:"
            for sql in conflict.sourceSQL {
                output += "\n    \(sql)"
            }
        }
        if !conflict.targetSQL.isEmpty {
            output += "\n  Target action:"
            for sql in conflict.targetSQL {
                output += "\n    \(sql)"
            }
        }
        FileHandle.standardError.write(Data(output.utf8))

        // Auto-accept non-destructive conflicts when --yes is set
        if autoAccept && !conflict.isDestructive {
            let msg = "\n  → Auto-accepted: apply source\n"
            FileHandle.standardError.write(Data(msg.utf8))
            return .applySource
        }

        let prompt = "\n  [s]ource wins  [t]arget wins  s[k]ip  > "
        FileHandle.standardError.write(Data(prompt.utf8))

        guard let line = readLine()?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() else {
            return .skip
        }

        switch line {
        case "s", "source":
            return .applySource
        case "t", "target":
            return .keepTarget
        case "k", "skip", "":
            return .skip
        default:
            let msg = "  Unknown choice '\(line)', skipping.\n"
            FileHandle.standardError.write(Data(msg.utf8))
            return .skip
        }
    }
}
