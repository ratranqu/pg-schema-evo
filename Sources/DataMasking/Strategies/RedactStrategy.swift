/// Replaces a value with a fixed redaction string.
public struct RedactStrategy: MaskingStrategy, Sendable {
    public static let name = "redact"

    /// The replacement string. Defaults to "***".
    public let replacement: String

    public init(replacement: String = "***") {
        self.replacement = replacement
    }

    public init(options: [String: String]) {
        self.replacement = options["value"] ?? "***"
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        replacement
    }
}
