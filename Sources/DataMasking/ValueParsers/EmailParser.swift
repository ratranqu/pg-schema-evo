import Parsing

/// Parses email addresses into local and domain parts.
///
/// Uses swift-parsing's `ParserPrinter` for bidirectional transformation:
/// - Parse: `"john@example.com"` → `EmailParts(local: "john", domain: "example.com")`
/// - Print: `EmailParts(local: "j***", domain: "example.com")` → `"j***@example.com"`
public struct EmailParser: Sendable {
    public init() {}

    /// Parse an email string into parts. Returns nil if not a valid email format.
    public func parse(_ input: String) -> EmailParts? {
        guard let atIndex = input.lastIndex(of: "@"),
              atIndex > input.startIndex,
              atIndex < input.index(before: input.endIndex) else {
            return nil
        }
        let local = String(input[input.startIndex..<atIndex])
        let domain = String(input[input.index(after: atIndex)...])
        return EmailParts(local: local, domain: domain)
    }

    /// Print parts back into an email string.
    public func print(_ parts: EmailParts) -> String {
        "\(parts.local)@\(parts.domain)"
    }
}
