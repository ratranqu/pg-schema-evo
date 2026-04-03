/// Parsed email components.
public struct EmailParts: Sendable, Equatable {
    public var local: String
    public var domain: String

    public init(local: String, domain: String) {
        self.local = local
        self.domain = domain
    }
}

/// Parsed phone number components.
public struct PhoneParts: Sendable, Equatable {
    public var segments: [String]
    public var separators: [String]

    public init(segments: [String], separators: [String]) {
        self.segments = segments
        self.separators = separators
    }

    /// Reconstruct the phone string.
    public func joined() -> String {
        var result = ""
        for (i, seg) in segments.enumerated() {
            result += seg
            if i < separators.count {
                result += separators[i]
            }
        }
        return result
    }
}

/// Parsed IPv4 address components.
public struct IPv4Parts: Sendable, Equatable {
    public var octets: [String]

    public init(octets: [String]) {
        self.octets = octets
    }

    /// Reconstruct the IP string.
    public func joined() -> String {
        octets.joined(separator: ".")
    }
}

/// Parsed credit card components.
public struct CreditCardParts: Sendable, Equatable {
    public var groups: [String]
    public var separator: String

    public init(groups: [String], separator: String) {
        self.groups = groups
        self.separator = separator
    }

    /// Reconstruct the credit card string.
    public func joined() -> String {
        groups.joined(separator: separator)
    }
}

/// Parsed SSN components (US format: AAA-BB-CCCC).
public struct SSNParts: Sendable, Equatable {
    public var area: String
    public var group: String
    public var serial: String

    public init(area: String, group: String, serial: String) {
        self.area = area
        self.group = group
        self.serial = serial
    }

    /// Reconstruct the SSN string.
    public func joined() -> String {
        "\(area)-\(group)-\(serial)"
    }
}
