/// A strategy that transforms a single column value for data masking.
///
/// Implement this protocol to provide custom masking strategies.
/// All strategies must be `Sendable` for concurrent row processing.
public protocol MaskingStrategy: Sendable {
    /// Unique identifier for this strategy (e.g., "hash", "fake", "redact").
    static var name: String { get }

    /// Mask a single string value.
    /// - Parameters:
    ///   - value: The original value to mask.
    ///   - context: Contextual information (table, column, options, seed).
    /// - Returns: The masked value, or `nil` to represent SQL NULL.
    func mask(_ value: String, context: MaskingContext) -> String?
}
