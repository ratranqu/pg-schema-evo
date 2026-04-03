import Testing
@testable import DataMasking

@Suite("NullStrategy Tests")
struct NullStrategyTests {
    let ctx = MaskingContext(table: "t", column: "c")

    @Test("Always returns nil")
    func alwaysNil() {
        let s = NullStrategy()
        #expect(s.mask("hello", context: ctx) == nil)
        #expect(s.mask("", context: ctx) == nil)
        #expect(s.mask("12345", context: ctx) == nil)
    }

    @Test("Strategy name is null")
    func name() {
        #expect(NullStrategy.name == "null")
    }
}
