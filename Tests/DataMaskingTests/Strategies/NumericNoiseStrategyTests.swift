import Testing
@testable import DataMasking

@Suite("NumericNoiseStrategy Tests")
struct NumericNoiseStrategyTests {
    let ctx = MaskingContext(table: "t", column: "c", seed: 42)

    @Test("Integer values stay close to original")
    func integerNoise() {
        let s = NumericNoiseStrategy(noiseFraction: 0.1)
        let result = s.mask("1000", context: ctx)!
        let value = Double(result)!
        #expect(value >= 900 && value <= 1100)
    }

    @Test("Preserves integer format")
    func integerFormat() {
        let s = NumericNoiseStrategy()
        let result = s.mask("42", context: ctx)!
        #expect(!result.contains("."))
    }

    @Test("Preserves decimal format")
    func decimalFormat() {
        let s = NumericNoiseStrategy()
        let result = s.mask("3.14", context: ctx)!
        #expect(result.contains("."))
        let parts = result.split(separator: ".")
        #expect(parts.count == 2)
        #expect(parts[1].count == 2) // same decimal places
    }

    @Test("Non-numeric passthrough")
    func nonNumeric() {
        let s = NumericNoiseStrategy()
        #expect(s.mask("hello", context: ctx) == "hello")
    }

    @Test("Deterministic by default")
    func deterministic() {
        let s = NumericNoiseStrategy()
        let a = s.mask("100", context: ctx)
        let b = s.mask("100", context: ctx)
        #expect(a == b)
    }

    @Test("Zero value stays zero-ish")
    func zeroValue() {
        let s = NumericNoiseStrategy(noiseFraction: 0.1)
        let result = s.mask("0", context: ctx)!
        let value = Double(result)!
        #expect(abs(value) <= 0.1) // 0 * (1 + noise) = 0
    }

    @Test("Negative values")
    func negativeValues() {
        let s = NumericNoiseStrategy(noiseFraction: 0.1)
        let result = s.mask("-500", context: ctx)!
        let value = Double(result)!
        #expect(value < 0) // should still be negative
        #expect(value >= -550 && value <= -450)
    }

    @Test("Options constructor")
    func options() {
        let s = NumericNoiseStrategy(options: ["noise": "0.2", "deterministic": "false"])
        #expect(s.noiseFraction == 0.2)
        #expect(s.deterministic == false)
    }

    @Test("Strategy name")
    func name() {
        #expect(NumericNoiseStrategy.name == "numeric-noise")
    }
}
