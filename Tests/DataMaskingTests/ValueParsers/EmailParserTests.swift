import Testing
@testable import DataMasking

@Suite("EmailParser Tests")
struct EmailParserTests {
    let parser = EmailParser()

    @Test("Parse simple email")
    func simple() {
        let parts = parser.parse("john@example.com")
        #expect(parts?.local == "john")
        #expect(parts?.domain == "example.com")
    }

    @Test("Round-trip parse/print")
    func roundTrip() {
        let email = "user.name+tag@sub.domain.com"
        let parts = parser.parse(email)!
        let printed = parser.print(parts)
        #expect(printed == email)
    }

    @Test("Parse with dots in local")
    func dotsInLocal() {
        let parts = parser.parse("first.last@example.com")
        #expect(parts?.local == "first.last")
    }

    @Test("Parse with plus addressing")
    func plusAddressing() {
        let parts = parser.parse("user+tag@example.com")
        #expect(parts?.local == "user+tag")
    }

    @Test("Print back modified parts")
    func printModified() {
        var parts = parser.parse("john@example.com")!
        parts.local = "masked"
        #expect(parser.print(parts) == "masked@example.com")
    }

    @Test("Invalid: no @")
    func noAt() {
        #expect(parser.parse("notanemail") == nil)
    }

    @Test("Invalid: nothing before @")
    func nothingBefore() {
        #expect(parser.parse("@domain.com") == nil)
    }

    @Test("Invalid: nothing after @")
    func nothingAfter() {
        #expect(parser.parse("user@") == nil)
    }

    @Test("Invalid: empty string")
    func empty() {
        #expect(parser.parse("") == nil)
    }

    @Test("Multiple @ uses last one")
    func multipleAt() {
        let parts = parser.parse("user@host@domain.com")
        #expect(parts?.local == "user@host")
        #expect(parts?.domain == "domain.com")
    }
}
