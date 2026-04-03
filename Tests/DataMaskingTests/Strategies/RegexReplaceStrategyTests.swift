import Testing
@testable import DataMasking

@Suite("RegexReplaceStrategy Tests")
struct RegexReplaceStrategyTests {
    let ctx = MaskingContext(table: "t", column: "c")

    @Test("Default replaces each character with *")
    func defaultPattern() {
        let s = RegexReplaceStrategy()
        #expect(s.mask("hello", context: ctx) == "*****")
    }

    @Test("Custom pattern")
    func customPattern() {
        let s = RegexReplaceStrategy(pattern: "[0-9]", replacement: "#")
        #expect(s.mask("abc123", context: ctx) == "abc###")
    }

    @Test("Group reference in replacement")
    func groupReference() {
        let s = RegexReplaceStrategy(pattern: "(\\w+)@(\\w+)", replacement: "***@$2")
        #expect(s.mask("john@example", context: ctx) == "***@example")
    }

    @Test("Full match mode")
    func fullMatch() {
        let s = RegexReplaceStrategy(pattern: "^[0-9]+$", replacement: "###", fullMatch: true)
        #expect(s.mask("12345", context: ctx) == "###")
        // Non-matching full string passes through
        #expect(s.mask("abc123", context: ctx) == "abc123")
    }

    @Test("Invalid regex returns original")
    func invalidRegex() {
        let s = RegexReplaceStrategy(pattern: "[invalid", replacement: "x")
        #expect(s.mask("test", context: ctx) == "test")
    }

    @Test("Options constructor")
    func options() {
        let s = RegexReplaceStrategy(options: [
            "pattern": "\\d",
            "replacement": "X",
            "full_match": "true",
        ])
        #expect(s.pattern == "\\d")
        #expect(s.replacement == "X")
        #expect(s.fullMatch == true)
    }

    @Test("Strategy name")
    func name() {
        #expect(RegexReplaceStrategy.name == "regex")
    }

    @Test("Empty string")
    func emptyString() {
        let s = RegexReplaceStrategy(pattern: ".", replacement: "*")
        #expect(s.mask("", context: ctx) == "")
    }
}
