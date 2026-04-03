import Foundation

/// Replaces values matching a regex pattern with a replacement string.
///
/// Options:
/// - `pattern`: The regex pattern to match.
/// - `replacement`: The replacement string (supports `$1`, `$2` group references).
/// - `full_match`: If "true", replaces the entire value only if the full string matches (default: false).
public struct RegexReplaceStrategy: MaskingStrategy, Sendable {
    public static let name = "regex"

    public let pattern: String
    public let replacement: String
    public let fullMatch: Bool

    public init(pattern: String = ".", replacement: String = "*", fullMatch: Bool = false) {
        self.pattern = pattern
        self.replacement = replacement
        self.fullMatch = fullMatch
    }

    public init(options: [String: String]) {
        self.pattern = options["pattern"] ?? "."
        self.replacement = options["replacement"] ?? "*"
        self.fullMatch = options["full_match"]
            .map { $0.lowercased() == "true" } ?? false
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return value
        }
        let range = NSRange(value.startIndex..., in: value)

        if fullMatch {
            let match = regex.firstMatch(in: value, range: range)
            guard let match, match.range == range else { return value }
        }

        return regex.stringByReplacingMatches(in: value, range: range, withTemplate: replacement)
    }
}
