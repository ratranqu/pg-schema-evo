/// Strategy for resolving schema conflicts between source and target.
public enum ConflictStrategy: String, Sendable, Codable, CaseIterable {
    /// Halt on any conflict (default — safe for CI).
    case fail
    /// Apply source definition for all conflicts (--ours).
    case sourceWins = "source-wins"
    /// Keep target definition for all conflicts (--theirs).
    case targetWins = "target-wins"
    /// Prompt the user per conflict (--manual).
    case interactive
    /// Skip all conflicting objects entirely.
    case skip
}
