import Testing
@testable import DataMasking

@Suite("PartialStrategy Tests")
struct PartialStrategyTests {
    @Test("Generic partial mask with default keep=1")
    func genericDefault() {
        let s = PartialStrategy()
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("secret", context: ctx) == "s*****")
    }

    @Test("Generic partial mask with keep=3")
    func genericKeep3() {
        let s = PartialStrategy(keep: 3)
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("secret", context: ctx) == "sec***")
    }

    @Test("Keep exceeds length returns original")
    func keepExceedsLength() {
        let s = PartialStrategy(keep: 10)
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("hi", context: ctx) == "hi")
    }

    @Test("Email type masks local part")
    func emailExplicit() {
        let s = PartialStrategy(keep: 1, valueType: "email")
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("john@example.com", context: ctx) == "j***@example.com")
    }

    @Test("Email inferred from column name")
    func emailInferred() {
        let s = PartialStrategy(keep: 2)
        let ctx = MaskingContext(table: "users", column: "email_address")
        #expect(s.mask("john@example.com", context: ctx) == "jo**@example.com")
    }

    @Test("Phone type masks segments after first")
    func phoneExplicit() {
        let s = PartialStrategy(valueType: "phone")
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("555-123-4567", context: ctx) == "555-***-****")
    }

    @Test("Phone inferred from column name")
    func phoneInferred() {
        let s = PartialStrategy()
        let ctx = MaskingContext(table: "t", column: "phone_number")
        #expect(s.mask("555-123-4567", context: ctx) == "555-***-****")
    }

    @Test("Custom mask character")
    func customMaskChar() {
        let s = PartialStrategy(keep: 2, maskChar: "#")
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("secret", context: ctx) == "se####")
    }

    @Test("Options constructor")
    func options() {
        let s = PartialStrategy(options: ["keep": "3", "mask_char": "X", "type": "email"])
        #expect(s.keep == 3)
        #expect(s.maskChar == "X")
        #expect(s.valueType == "email")
    }

    @Test("Strategy name")
    func name() {
        #expect(PartialStrategy.name == "partial")
    }

    @Test("Empty string")
    func emptyString() {
        let s = PartialStrategy()
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("", context: ctx) == "")
    }

    @Test("Email without @ falls back to generic")
    func invalidEmail() {
        let s = PartialStrategy(valueType: "email")
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("notanemail", context: ctx) == "n*********")
    }
}
