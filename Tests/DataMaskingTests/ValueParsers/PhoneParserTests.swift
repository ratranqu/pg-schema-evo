import Testing
@testable import DataMasking

@Suite("PhoneParser Tests")
struct PhoneParserTests {
    let parser = PhoneParser()

    @Test("Parse dash-separated phone")
    func dashSeparated() {
        let parts = parser.parse("555-123-4567")!
        #expect(parts.segments == ["555", "123", "4567"])
        #expect(parts.separators == ["-", "-"])
    }

    @Test("Round-trip dash-separated")
    func roundTripDash() {
        let phone = "555-123-4567"
        let parts = parser.parse(phone)!
        #expect(parser.print(parts) == phone)
    }

    @Test("Parse space-separated phone")
    func spaceSeparated() {
        let parts = parser.parse("555 123 4567")!
        #expect(parts.segments == ["555", "123", "4567"])
    }

    @Test("Parse with country code")
    func countryCode() {
        let parts = parser.parse("+1 555 123 4567")!
        #expect(parts.segments.count >= 3)
    }

    @Test("Parse parenthesized area code")
    func parenthesized() {
        let parts = parser.parse("(555) 123-4567")!
        #expect(parts.segments.contains("555"))
        #expect(parts.segments.contains("123"))
        #expect(parts.segments.contains("4567"))
    }

    @Test("Parenthesized area code segments parsed correctly")
    func parenthesizedSegments() {
        let parts = parser.parse("(555) 123-4567")!
        #expect(parts.segments == ["555", "123", "4567"])
    }

    @Test("Invalid: empty string")
    func empty() {
        #expect(parser.parse("") == nil)
    }

    @Test("Invalid: no digits")
    func noDigits() {
        #expect(parser.parse("hello") == nil)
    }

    @Test("Dot-separated phone")
    func dotSeparated() {
        let parts = parser.parse("555.123.4567")!
        #expect(parts.segments == ["555", "123", "4567"])
    }
}
