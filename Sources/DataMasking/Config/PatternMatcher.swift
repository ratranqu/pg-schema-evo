/// Matches table.column references against glob-like patterns.
///
/// Supported patterns:
/// - `*.column` — any table, specific column
/// - `table.*` — specific table, any column
/// - `table.column` — exact match
/// - `*.*` — matches everything
public struct PatternMatcher: Sendable {
    public init() {}

    /// Check if a `table.column` pair matches the given pattern.
    public func matches(pattern: String, table: String, column: String) -> Bool {
        let parts = pattern.split(separator: ".", maxSplits: 1)

        switch parts.count {
        case 2:
            let patTable = String(parts[0])
            let patColumn = String(parts[1])
            return matchComponent(patTable, table) && matchComponent(patColumn, column)
        case 1:
            // Pattern without dot — match against column name only
            return matchComponent(String(parts[0]), column)
        default:
            return false
        }
    }

    private func matchComponent(_ pattern: String, _ value: String) -> Bool {
        if pattern == "*" { return true }
        return pattern == value
    }
}
