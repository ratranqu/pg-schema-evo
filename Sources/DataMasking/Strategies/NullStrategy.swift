/// Replaces any value with NULL.
public struct NullStrategy: MaskingStrategy, Sendable {
    public static let name = "null"

    public init() {}

    public func mask(_ value: String, context: MaskingContext) -> String? {
        nil
    }
}
