import Parsing

/// Parses US Social Security Numbers (format: AAA-BB-CCCC).
public struct SSNParser: Sendable {
    public init() {}

    /// Parse an SSN string into parts.
    public func parse(_ input: String) -> SSNParts? {
        let parts = input.split(separator: "-", omittingEmptySubsequences: false)
        guard parts.count == 3,
              parts[0].count == 3, parts[0].allSatisfy(\.isNumber),
              parts[1].count == 2, parts[1].allSatisfy(\.isNumber),
              parts[2].count == 4, parts[2].allSatisfy(\.isNumber) else {
            return nil
        }
        return SSNParts(area: String(parts[0]), group: String(parts[1]), serial: String(parts[2]))
    }

    /// Print parts back into an SSN string.
    public func print(_ parts: SSNParts) -> String {
        parts.joined()
    }
}
