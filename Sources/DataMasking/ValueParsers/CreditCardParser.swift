import Parsing

/// Parses credit card numbers into groups.
///
/// Handles formats like: `4111-1111-1111-1111`, `4111 1111 1111 1111`
public struct CreditCardParser: Sendable {
    public init() {}

    /// Parse a credit card string into groups and separator.
    public func parse(_ input: String) -> CreditCardParts? {
        // Detect separator
        let separator: String
        if input.contains("-") {
            separator = "-"
        } else if input.contains(" ") {
            separator = " "
        } else {
            // No separator — treat whole string as one group
            guard input.allSatisfy(\.isNumber), input.count >= 13 else { return nil }
            return CreditCardParts(groups: [input], separator: "")
        }

        let groups = input.split(separator: Character(separator), omittingEmptySubsequences: false)
            .map(String.init)
        guard groups.count >= 2, groups.allSatisfy({ $0.allSatisfy(\.isNumber) && !$0.isEmpty }) else {
            return nil
        }
        return CreditCardParts(groups: groups, separator: separator)
    }

    /// Print parts back into a credit card string.
    public func print(_ parts: CreditCardParts) -> String {
        parts.joined()
    }
}
