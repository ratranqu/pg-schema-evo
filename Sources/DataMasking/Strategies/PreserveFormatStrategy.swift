import Foundation

/// Masks a value while preserving its character-class structure.
///
/// - Digits are replaced with random digits
/// - Letters are replaced with random letters (preserving case)
/// - All other characters (separators, punctuation) are preserved
///
/// Example: `555-123-4567` → `738-491-2056`
///
/// Uses deterministic PRNG seeded from context for reproducibility.
///
/// Options:
/// - `deterministic`: If "false", uses random replacement (default: true)
public struct PreserveFormatStrategy: MaskingStrategy, Sendable {
    public static let name = "preserve-format"

    public let deterministic: Bool

    public init(deterministic: Bool = true) {
        self.deterministic = deterministic
    }

    public init(options: [String: String]) {
        self.deterministic = options["deterministic"]
            .map { $0.lowercased() != "false" } ?? true
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        var seed = deterministic ? fnv1a64(value + String(context.seed)) : UInt64.random(in: 0...UInt64.max)
        var result = ""
        result.reserveCapacity(value.count)

        for ch in value {
            if ch.isNumber {
                let digit = nextRandom(&seed) % 10
                result.append(Character(String(digit)))
            } else if ch.isLetter {
                let base: UInt32 = ch.isUppercase ? 65 : 97  // A or a
                let letter = base + UInt32(nextRandom(&seed) % 26)
                result.append(Character(UnicodeScalar(letter)!))
            } else {
                result.append(ch)
            }
        }
        return result
    }

    /// Simple xorshift64 PRNG step — returns a value in [0, UInt64.max].
    private func nextRandom(_ state: inout UInt64) -> UInt64 {
        state ^= state << 13
        state ^= state >> 7
        state ^= state << 17
        return state
    }
}
