import Foundation

/// Generates fake realistic-looking replacement values.
///
/// Uses deterministic selection from built-in word lists.
///
/// Options:
/// - `type`: The type of fake data (`name`, `email`, `phone`, `address`, `company`, `text`).
///           If not specified, inferred from column name.
/// - `locale`: Locale hint (currently only `en` supported, reserved for future expansion).
public struct FakeStrategy: MaskingStrategy, Sendable {
    public static let name = "fake"

    public let dataType: String?
    public let locale: String

    public init(dataType: String? = nil, locale: String = "en") {
        self.dataType = dataType
        self.locale = locale
    }

    public init(options: [String: String]) {
        self.dataType = options["type"]
        self.locale = options["locale"] ?? "en"
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        let effectiveType = dataType ?? inferType(column: context.column)
        var seed = fnv1a64(value + String(context.seed))

        switch effectiveType {
        case "name":
            return fakeName(seed: &seed)
        case "first_name":
            return pick(from: Self.firstNames, seed: &seed)
        case "last_name":
            return pick(from: Self.lastNames, seed: &seed)
        case "email":
            return fakeEmail(seed: &seed)
        case "phone":
            return fakePhone(seed: &seed)
        case "address":
            return fakeAddress(seed: &seed)
        case "company":
            return pick(from: Self.companies, seed: &seed)
        case "city":
            return pick(from: Self.cities, seed: &seed)
        default:
            return fakeText(length: value.count, seed: &seed)
        }
    }

    private func inferType(column: String) -> String {
        let col = column.lowercased()
        if col.contains("email") { return "email" }
        if col.contains("phone") || col.contains("mobile") { return "phone" }
        if col.contains("first_name") || col.contains("firstname") { return "first_name" }
        if col.contains("last_name") || col.contains("lastname") { return "last_name" }
        if col.contains("name") { return "name" }
        if col.contains("address") || col.contains("street") { return "address" }
        if col.contains("company") || col.contains("org") { return "company" }
        if col.contains("city") { return "city" }
        return "text"
    }

    // MARK: - Generators

    private func fakeName(seed: inout UInt64) -> String {
        let first = pick(from: Self.firstNames, seed: &seed)
        let last = pick(from: Self.lastNames, seed: &seed)
        return "\(first) \(last)"
    }

    private func fakeEmail(seed: inout UInt64) -> String {
        let first = pick(from: Self.firstNames, seed: &seed).lowercased()
        let last = pick(from: Self.lastNames, seed: &seed).lowercased()
        let domain = pick(from: Self.domains, seed: &seed)
        return "\(first).\(last)@\(domain)"
    }

    private func fakePhone(seed: inout UInt64) -> String {
        let a = nextRandom(&seed) % 900 + 100
        let b = nextRandom(&seed) % 900 + 100
        let c = nextRandom(&seed) % 9000 + 1000
        return "\(a)-\(b)-\(c)"
    }

    private func fakeAddress(seed: inout UInt64) -> String {
        let num = nextRandom(&seed) % 9999 + 1
        let street = pick(from: Self.streets, seed: &seed)
        let suffix = pick(from: Self.streetSuffixes, seed: &seed)
        return "\(num) \(street) \(suffix)"
    }

    private func fakeText(length: Int, seed: inout UInt64) -> String {
        let words = Self.loremWords
        var result = ""
        while result.count < length {
            if !result.isEmpty { result += " " }
            result += pick(from: words, seed: &seed)
        }
        return String(result.prefix(max(length, 1)))
    }

    private func pick(from list: [String], seed: inout UInt64) -> String {
        let idx = Int(nextRandom(&seed) % UInt64(list.count))
        return list[idx]
    }

    private func nextRandom(_ seed: inout UInt64) -> UInt64 {
        seed ^= seed << 13
        seed ^= seed >> 7
        seed ^= seed << 17
        return seed
    }

    // MARK: - Word Lists

    static let firstNames = [
        "Alice", "Bob", "Carol", "David", "Emma", "Frank", "Grace", "Henry",
        "Iris", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul",
        "Quinn", "Rose", "Sam", "Tara", "Uma", "Vera", "Will", "Xena", "Yuki", "Zara",
    ]

    static let lastNames = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
        "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
        "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    ]

    static let domains = [
        "example.com", "test.org", "sample.net", "demo.io", "mock.dev",
        "fakeco.com", "placeholder.org", "dummy.net",
    ]

    static let companies = [
        "Acme Corp", "Globex Inc", "Initech", "Umbrella Ltd", "Soylent Corp",
        "Wonka Industries", "Sterling Cooper", "Stark Industries", "Wayne Enterprises",
        "Cyberdyne Systems", "Oscorp", "LexCorp", "Tyrell Corp", "Weyland-Yutani",
    ]

    static let cities = [
        "Springfield", "Riverside", "Fairview", "Madison", "Georgetown",
        "Clinton", "Arlington", "Salem", "Franklin", "Chester",
    ]

    static let streets = [
        "Main", "Oak", "Maple", "Cedar", "Elm", "Pine", "Walnut", "Washington",
        "Park", "Lake", "Hill", "Sunset", "River", "Forest", "Spring",
    ]

    static let streetSuffixes = [
        "St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Ct", "Way", "Pl", "Cir",
    ]

    static let loremWords = [
        "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
        "elit", "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore",
        "et", "dolore", "magna", "aliqua", "enim", "ad", "minim", "veniam",
    ]
}
