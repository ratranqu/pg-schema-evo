import Testing
@testable import DataMasking

@Suite("RedactStrategy Tests")
struct RedactStrategyTests {
    let ctx = MaskingContext(table: "t", column: "c")

    @Test("Default replacement is ***")
    func defaultReplacement() {
        let s = RedactStrategy()
        #expect(s.mask("secret", context: ctx) == "***")
        #expect(s.mask("", context: ctx) == "***")
    }

    @Test("Custom replacement via init")
    func customInit() {
        let s = RedactStrategy(replacement: "REDACTED")
        #expect(s.mask("secret", context: ctx) == "REDACTED")
    }

    @Test("Custom replacement via options")
    func customOptions() {
        let s = RedactStrategy(options: ["value": "[HIDDEN]"])
        #expect(s.mask("secret", context: ctx) == "[HIDDEN]")
    }

    @Test("Empty options uses default")
    func emptyOptions() {
        let s = RedactStrategy(options: [:])
        #expect(s.mask("x", context: ctx) == "***")
    }

    @Test("Strategy name is redact")
    func name() {
        #expect(RedactStrategy.name == "redact")
    }
}
