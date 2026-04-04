import Foundation
import Logging

/// Applies a conflict resolution strategy to a `ConflictReport` and produces
/// a list of `ConflictResolution` values that determine which SQL to execute.
public struct ConflictResolver: Sendable {
    private let strategy: ConflictStrategy
    private let force: Bool
    private let logger: Logger

    public init(strategy: ConflictStrategy, force: Bool, logger: Logger) {
        self.strategy = strategy
        self.force = force
        self.logger = logger
    }

    /// Resolve all conflicts using the configured non-interactive strategy.
    /// Throws `conflictsDetected` for `.fail` strategy when conflicts exist.
    /// Throws `destructiveActionBlocked` for `.sourceWins` when destructive changes
    /// are present and `force` is false.
    public func resolve(report: ConflictReport) throws -> [ConflictResolution] {
        guard !report.isEmpty else { return [] }

        switch strategy {
        case .fail:
            throw PGSchemaEvoError.conflictsDetected(
                count: report.count,
                destructive: report.destructiveConflicts.count
            )

        case .sourceWins:
            // Check for destructive conflicts when --force is not set
            if !force {
                let destructive = report.destructiveConflicts
                if !destructive.isEmpty {
                    let descriptions = destructive.map { conflict in
                        "\(conflict.objectIdentifier) — \(conflict.description)"
                    }
                    throw PGSchemaEvoError.destructiveActionBlocked(descriptions: descriptions)
                }
            }
            return report.conflicts.map { conflict in
                ConflictResolution(conflictId: conflict.id, choice: .applySource)
            }

        case .targetWins:
            return report.conflicts.map { conflict in
                ConflictResolution(conflictId: conflict.id, choice: .keepTarget)
            }

        case .skip:
            return report.conflicts.map { conflict in
                ConflictResolution(conflictId: conflict.id, choice: .skip)
            }

        case .interactive:
            // Interactive requires a prompter — use resolveInteractive instead
            logger.warning("Interactive strategy requires a prompter. Falling back to fail.")
            throw PGSchemaEvoError.conflictsDetected(
                count: report.count,
                destructive: report.destructiveConflicts.count
            )
        }
    }

    /// Resolve conflicts interactively using the provided prompter.
    public func resolveInteractive(
        report: ConflictReport,
        prompter: ConflictPrompter
    ) async throws -> [ConflictResolution] {
        guard !report.isEmpty else { return [] }

        var resolutions: [ConflictResolution] = []

        for (index, conflict) in report.conflicts.enumerated() {
            // For destructive conflicts without --force, block even in interactive mode
            if conflict.isDestructive && !force {
                let msg = "\n  \u{1B}[31m✗ Destructive change blocked\u{1B}[0m — use --force to allow: \(conflict.description)\n"
                FileHandle.standardError.write(Data(msg.utf8))
                resolutions.append(ConflictResolution(
                    conflictId: conflict.id,
                    choice: .skip
                ))
                continue
            }

            let choice = await prompter.prompt(
                conflict: conflict,
                index: index + 1,
                total: report.count
            )
            resolutions.append(ConflictResolution(
                conflictId: conflict.id,
                choice: choice
            ))
        }

        return resolutions
    }

    /// Resolve conflicts from a previously saved conflict file.
    /// Returns matched resolutions. Throws on unresolved conflicts if strategy is `.fail`.
    public static func resolveFromFile(
        path: String,
        report: ConflictReport,
        strategy: ConflictStrategy,
        logger: Logger
    ) throws -> [ConflictResolution] {
        let fileResolutions = try ConflictFileIO.readResolutions(from: path)
        let fileConflicts = try ConflictFileIO.readConflicts(from: path)
        let (matched, unresolved) = ConflictFileIO.matchResolutions(
            fileResolutions: fileResolutions,
            fileConflicts: fileConflicts,
            report: report
        )
        if !unresolved.isEmpty {
            logger.warning("\(unresolved.count) new conflict(s) not in resolution file")
            if strategy == .fail {
                throw PGSchemaEvoError.conflictsDetected(
                    count: unresolved.count,
                    destructive: unresolved.filter(\.isDestructive).count
                )
            }
        }
        return matched
    }

    /// Collect the SQL statements to execute based on resolved conflicts.
    /// Returns only the SQL for conflicts resolved as `applySource`.
    public static func sqlForResolutions(
        _ resolutions: [ConflictResolution],
        report: ConflictReport
    ) -> [String] {
        let conflictMap = Dictionary(uniqueKeysWithValues: report.conflicts.map { ($0.id, $0) })
        var sql: [String] = []

        for resolution in resolutions {
            guard resolution.choice == .applySource,
                  let conflict = conflictMap[resolution.conflictId] else {
                continue
            }
            sql.append(contentsOf: conflict.sourceSQL)
        }

        return sql
    }
}
