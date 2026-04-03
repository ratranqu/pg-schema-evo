import Testing
@testable import DataMasking

@Suite("SSNParser Tests")
struct SSNParserTests {
    let parser = SSNParser()

    @Test("Parse valid SSN")
    func valid() {
        let parts = parser.parse("123-45-6789")!
        #expect(parts.area == "123")
        #expect(parts.group == "45")
        #expect(parts.serial == "6789")
    }

    @Test("Round-trip")
    func roundTrip() {
        let ssn = "999-88-7777"
        let parts = parser.parse(ssn)!
        #expect(parser.print(parts) == ssn)
    }

    @Test("Invalid: wrong segment lengths")
    func wrongLengths() {
        #expect(parser.parse("12-345-6789") == nil)
        #expect(parser.parse("1234-56-789") == nil)
    }

    @Test("Invalid: no dashes")
    func noDashes() {
        #expect(parser.parse("123456789") == nil)
    }

    @Test("Invalid: letters")
    func letters() {
        #expect(parser.parse("abc-de-fghi") == nil)
    }

    @Test("Invalid: empty")
    func empty() {
        #expect(parser.parse("") == nil)
    }

    @Test("Print modified parts")
    func printModified() {
        var parts = parser.parse("123-45-6789")!
        parts.area = "***"
        parts.group = "**"
        #expect(parser.print(parts) == "***-**-6789")
    }
}
