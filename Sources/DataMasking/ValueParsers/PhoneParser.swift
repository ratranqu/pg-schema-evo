import Parsing

/// Parses phone numbers into digit segments and separators.
///
/// Handles formats like: `555-123-4567`, `(555) 123-4567`, `555.123.4567`, `+1 555 123 4567`
public struct PhoneParser: Sendable {
    public init() {}

    /// Parse a phone string into segments and separators.
    public func parse(_ input: String) -> PhoneParts? {
        var segments: [String] = []
        var separators: [String] = []
        var currentDigits = ""
        var currentSep = ""
        var inDigits = false

        for ch in input {
            if ch.isNumber {
                if !inDigits && !currentSep.isEmpty {
                    separators.append(currentSep)
                    currentSep = ""
                }
                if !inDigits && !currentDigits.isEmpty {
                    segments.append(currentDigits)
                    currentDigits = ""
                }
                currentDigits.append(ch)
                inDigits = true
            } else {
                if inDigits {
                    segments.append(currentDigits)
                    currentDigits = ""
                    inDigits = false
                }
                currentSep.append(ch)
            }
        }
        if !currentDigits.isEmpty {
            segments.append(currentDigits)
        }

        guard !segments.isEmpty else { return nil }
        return PhoneParts(segments: segments, separators: separators)
    }

    /// Print parts back into a phone string.
    public func print(_ parts: PhoneParts) -> String {
        parts.joined()
    }
}
