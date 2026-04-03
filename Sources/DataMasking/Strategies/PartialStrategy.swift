import Foundation

/// Masks part of a value while keeping some characters visible.
///
/// Examples:
/// - Email: `john@example.com` → `j***@example.com` (keep: 1)
/// - Phone: `555-123-4567` → `555-***-****`
/// - Generic: `secret` → `s*****` (keep: 1)
///
/// Options:
/// - `keep`: Number of leading characters to preserve (default: 1)
/// - `mask_char`: Character used for masking (default: `*`)
/// - `type`: Value type hint for structured parsing (`email`, `phone`, etc.)
public struct PartialStrategy: MaskingStrategy, Sendable {
    public static let name = "partial"

    public let keep: Int
    public let maskChar: Character
    public let valueType: String?

    public init(keep: Int = 1, maskChar: Character = "*", valueType: String? = nil) {
        self.keep = keep
        self.maskChar = maskChar
        self.valueType = valueType
    }

    public init(options: [String: String]) {
        self.keep = options["keep"].flatMap(Int.init) ?? 1
        self.maskChar = options["mask_char"]?.first ?? "*"
        self.valueType = options["type"]
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        let effectiveType = valueType ?? inferType(value: value, column: context.column)

        switch effectiveType {
        case "email":
            return maskEmail(value)
        case "phone":
            return maskPhone(value)
        default:
            return maskGeneric(value)
        }
    }

    private func maskEmail(_ value: String) -> String {
        let parser = EmailParser()
        guard var parts = parser.parse(value) else {
            return maskGeneric(value)
        }
        parts.local = maskString(parts.local)
        return parser.print(parts)
    }

    private func maskPhone(_ value: String) -> String {
        let parser = PhoneParser()
        guard var parts = parser.parse(value) else {
            return maskGeneric(value)
        }
        // Keep first segment, mask the rest
        for i in 1..<parts.segments.count {
            parts.segments[i] = String(repeating: maskChar, count: parts.segments[i].count)
        }
        return parser.print(parts)
    }

    private func maskGeneric(_ value: String) -> String {
        maskString(value)
    }

    private func maskString(_ s: String) -> String {
        guard s.count > keep else { return s }
        let visible = s.prefix(keep)
        let masked = String(repeating: maskChar, count: s.count - keep)
        return String(visible) + masked
    }

    private func inferType(value: String, column: String) -> String? {
        let col = column.lowercased()
        if col.contains("email") { return "email" }
        if col.contains("phone") || col.contains("mobile") || col.contains("fax") { return "phone" }
        if value.contains("@") && value.contains(".") { return "email" }
        return nil
    }
}
