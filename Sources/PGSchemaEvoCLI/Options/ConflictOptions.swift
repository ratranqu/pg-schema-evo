import ArgumentParser
import PGSchemaEvoCore

struct ConflictOptions: ParsableArguments {
    @Option(name: .long, help: "Conflict resolution strategy: fail, source-wins, target-wins, interactive, skip (default: fail)")
    var conflictStrategy: String?

    @Flag(name: .long, help: "Alias for --conflict-strategy source-wins (source wins all conflicts)")
    var ours: Bool = false

    @Flag(name: .long, help: "Alias for --conflict-strategy target-wins (target wins all conflicts)")
    var theirs: Bool = false

    @Flag(name: .long, help: "Alias for --conflict-strategy interactive (prompt per conflict)")
    var manual: Bool = false

    @Flag(name: .long, help: "Auto-accept non-destructive resolutions in interactive mode")
    var yes: Bool = false

    @Option(name: .long, help: "Write conflict report to file for offline review")
    var conflictFile: String?

    @Option(name: .long, help: "Apply resolutions from a previously generated conflict file")
    var resolveFrom: String?

    /// Resolve the effective conflict strategy from flags and option.
    func resolvedStrategy() throws -> ConflictStrategy {
        // Flag aliases take precedence over --conflict-strategy
        let flagCount = [ours, theirs, manual].filter { $0 }.count
        if flagCount > 1 {
            throw ValidationError("Only one of --ours, --theirs, --manual can be specified")
        }

        if ours { return .sourceWins }
        if theirs { return .targetWins }
        if manual { return .interactive }

        if let strategyStr = conflictStrategy {
            guard let strategy = ConflictStrategy(rawValue: strategyStr) else {
                let valid = ConflictStrategy.allCases.map(\.rawValue).joined(separator: ", ")
                throw ValidationError("Unknown conflict strategy '\(strategyStr)'. Valid: \(valid)")
            }
            return strategy
        }

        return .fail
    }

    /// Whether any conflict-related option was explicitly specified by the user.
    var isExplicit: Bool {
        conflictStrategy != nil || ours || theirs || manual || conflictFile != nil || resolveFrom != nil
    }
}
