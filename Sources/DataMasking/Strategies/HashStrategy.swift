import Foundation

/// Produces a deterministic hash of the input value.
///
/// Deterministic: same input + same seed always produces the same output,
/// which preserves referential integrity across foreign key relationships.
///
/// Uses FNV-1a 64-bit hash for cross-platform consistency (no CryptoKit dependency).
public struct HashStrategy: MaskingStrategy, Sendable {
    public static let name = "hash"

    /// Number of hex characters in the output. Defaults to 16.
    public let length: Int
    /// Optional prefix prepended to the hash output.
    public let prefix: String
    /// Optional salt mixed into the hash for added security.
    public let salt: String

    public init(length: Int = 16, prefix: String = "", salt: String = "") {
        self.length = length
        self.prefix = prefix
        self.salt = salt
    }

    public init(options: [String: String]) {
        self.length = options["length"].flatMap(Int.init) ?? 16
        self.prefix = options["prefix"] ?? ""
        self.salt = options["salt"] ?? ""
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        let input = salt + value
        let hash = fnv1a64(input)
        // Produce two hashes if we need more than 16 hex chars
        let hex: String
        if length <= 16 {
            hex = String(hash, radix: 16)
        } else {
            let hash2 = fnv1a64(input + String(hash))
            hex = String(hash, radix: 16) + String(hash2, radix: 16)
        }
        let padded = hex.count < length
            ? String(repeating: "0", count: length - hex.count) + hex
            : String(hex.prefix(length))
        return prefix + padded
    }
}

/// FNV-1a 64-bit hash — fast, deterministic, no external dependency.
func fnv1a64(_ string: String) -> UInt64 {
    var hash: UInt64 = 0xcbf29ce484222325
    for byte in string.utf8 {
        hash ^= UInt64(byte)
        hash &*= 0x100000001b3
    }
    return hash
}
