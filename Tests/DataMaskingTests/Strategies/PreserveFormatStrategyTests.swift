import Testing
@testable import DataMasking

@Suite("PreserveFormatStrategy Tests")
struct PreserveFormatStrategyTests {
    let ctx = MaskingContext(table: "t", column: "c", seed: 42)

    @Test("Preserves digit positions")
    func preserveDigits() {
        let s = PreserveFormatStrategy()
        let result = s.mask("555-123-4567", context: ctx)!
        // Check format: DDD-DDD-DDDD
        let parts = result.split(separator: "-")
        #expect(parts.count == 3)
        #expect(parts[0].count == 3 && parts[0].allSatisfy(\.isNumber))
        #expect(parts[1].count == 3 && parts[1].allSatisfy(\.isNumber))
        #expect(parts[2].count == 4 && parts[2].allSatisfy(\.isNumber))
    }

    @Test("Preserves letter positions and case")
    func preserveLetters() {
        let s = PreserveFormatStrategy()
        let result = s.mask("Ab12", context: ctx)!
        #expect(result.count == 4)
        #expect(result.first!.isUppercase)
        #expect(Array(result)[1].isLowercase)
        #expect(Array(result)[2].isNumber)
        #expect(Array(result)[3].isNumber)
    }

    @Test("Preserves separators")
    func preserveSeparators() {
        let s = PreserveFormatStrategy()
        let result = s.mask("(555) 123-4567", context: ctx)!
        #expect(result.hasPrefix("("))
        #expect(result.contains(") "))
        #expect(result.contains("-"))
    }

    @Test("Same length as input")
    func sameLength() {
        let s = PreserveFormatStrategy()
        let input = "Hello World 123!"
        let result = s.mask(input, context: ctx)!
        #expect(result.count == input.count)
    }

    @Test("Deterministic by default")
    func deterministic() {
        let s = PreserveFormatStrategy()
        let a = s.mask("test123", context: ctx)
        let b = s.mask("test123", context: ctx)
        #expect(a == b)
    }

    @Test("Different from input")
    func differentFromInput() {
        let s = PreserveFormatStrategy()
        // For a sufficiently long string, it's extremely unlikely to produce the same result
        let result = s.mask("ABCDEFGHIJ1234567890", context: ctx)!
        #expect(result != "ABCDEFGHIJ1234567890")
    }

    @Test("Options constructor")
    func options() {
        let s = PreserveFormatStrategy(options: ["deterministic": "false"])
        #expect(s.deterministic == false)
    }

    @Test("Strategy name")
    func name() {
        #expect(PreserveFormatStrategy.name == "preserve-format")
    }

    @Test("Empty string")
    func emptyString() {
        let s = PreserveFormatStrategy()
        #expect(s.mask("", context: ctx) == "")
    }

    @Test("Only separators preserved as-is")
    func onlySeparators() {
        let s = PreserveFormatStrategy()
        #expect(s.mask("---", context: ctx) == "---")
    }
}
