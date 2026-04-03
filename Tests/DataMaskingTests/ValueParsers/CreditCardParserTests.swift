import Testing
@testable import DataMasking

@Suite("CreditCardParser Tests")
struct CreditCardParserTests {
    let parser = CreditCardParser()

    @Test("Parse dash-separated card")
    func dashSeparated() {
        let parts = parser.parse("4111-1111-1111-1111")!
        #expect(parts.groups == ["4111", "1111", "1111", "1111"])
        #expect(parts.separator == "-")
    }

    @Test("Parse space-separated card")
    func spaceSeparated() {
        let parts = parser.parse("4111 1111 1111 1111")!
        #expect(parts.groups == ["4111", "1111", "1111", "1111"])
        #expect(parts.separator == " ")
    }

    @Test("Parse no-separator card")
    func noSeparator() {
        let parts = parser.parse("4111111111111111")!
        #expect(parts.groups == ["4111111111111111"])
        #expect(parts.separator == "")
    }

    @Test("Round-trip")
    func roundTrip() {
        let card = "4111-1111-1111-1111"
        let parts = parser.parse(card)!
        #expect(parser.print(parts) == card)
    }

    @Test("Invalid: contains letters")
    func letters() {
        #expect(parser.parse("4111-abcd-1111-1111") == nil)
    }

    @Test("Invalid: too short")
    func tooShort() {
        #expect(parser.parse("411111") == nil)
    }

    @Test("Invalid: empty")
    func empty() {
        #expect(parser.parse("") == nil)
    }

    @Test("Three groups (Amex-like)")
    func threeGroups() {
        let parts = parser.parse("3782-822463-10005")!
        #expect(parts.groups.count == 3)
    }
}
