import Parsing

/// Parses IPv4 addresses into octets.
///
/// Parse: `"192.168.1.1"` → `IPv4Parts(octets: ["192", "168", "1", "1"])`
/// Print: reconstructs `"192.168.1.1"`
public struct IPAddressParser: Sendable {
    public init() {}

    /// Parse an IPv4 address into octets. Returns nil if not valid format.
    public func parse(_ input: String) -> IPv4Parts? {
        let parts = input.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count == 4 else { return nil }
        var octets: [String] = []
        for part in parts {
            guard !part.isEmpty, part.allSatisfy(\.isNumber),
                  let val = Int(part), val >= 0, val <= 255 else {
                return nil
            }
            octets.append(String(part))
        }
        return IPv4Parts(octets: octets)
    }

    /// Print parts back into an IP string.
    public func print(_ parts: IPv4Parts) -> String {
        parts.joined()
    }
}
