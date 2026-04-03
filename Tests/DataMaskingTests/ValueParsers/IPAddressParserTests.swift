import Testing
@testable import DataMasking

@Suite("IPAddressParser Tests")
struct IPAddressParserTests {
    let parser = IPAddressParser()

    @Test("Parse valid IPv4")
    func validIPv4() {
        let parts = parser.parse("192.168.1.1")!
        #expect(parts.octets == ["192", "168", "1", "1"])
    }

    @Test("Round-trip")
    func roundTrip() {
        let ip = "10.0.0.255"
        let parts = parser.parse(ip)!
        #expect(parser.print(parts) == ip)
    }

    @Test("All zeros")
    func allZeros() {
        let parts = parser.parse("0.0.0.0")!
        #expect(parts.octets == ["0", "0", "0", "0"])
    }

    @Test("Max values")
    func maxValues() {
        let parts = parser.parse("255.255.255.255")!
        #expect(parts.octets.count == 4)
    }

    @Test("Invalid: octet > 255")
    func octetTooLarge() {
        #expect(parser.parse("256.1.1.1") == nil)
    }

    @Test("Invalid: too few octets")
    func tooFewOctets() {
        #expect(parser.parse("192.168.1") == nil)
    }

    @Test("Invalid: too many octets")
    func tooManyOctets() {
        #expect(parser.parse("1.2.3.4.5") == nil)
    }

    @Test("Invalid: empty string")
    func empty() {
        #expect(parser.parse("") == nil)
    }

    @Test("Invalid: letters")
    func letters() {
        #expect(parser.parse("abc.def.ghi.jkl") == nil)
    }

    @Test("Invalid: empty octet")
    func emptyOctet() {
        #expect(parser.parse("1..2.3") == nil)
    }
}
