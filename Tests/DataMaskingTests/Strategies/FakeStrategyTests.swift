import Testing
@testable import DataMasking

@Suite("FakeStrategy Tests")
struct FakeStrategyTests {
    let ctx = MaskingContext(table: "users", column: "name", seed: 42)

    @Test("Fake name produces two words")
    func fakeName() {
        let s = FakeStrategy(dataType: "name")
        let result = s.mask("John Doe", context: ctx)!
        let parts = result.split(separator: " ")
        #expect(parts.count == 2)
    }

    @Test("Fake email contains @")
    func fakeEmail() {
        let s = FakeStrategy(dataType: "email")
        let result = s.mask("john@example.com", context: ctx)!
        #expect(result.contains("@"))
        #expect(result.contains("."))
    }

    @Test("Fake phone has digit segments")
    func fakePhone() {
        let s = FakeStrategy(dataType: "phone")
        let result = s.mask("555-123-4567", context: ctx)!
        let parts = result.split(separator: "-")
        #expect(parts.count == 3)
        #expect(parts.allSatisfy { $0.allSatisfy(\.isNumber) })
    }

    @Test("Fake address starts with number")
    func fakeAddress() {
        let s = FakeStrategy(dataType: "address")
        let result = s.mask("123 Main St", context: ctx)!
        #expect(result.first?.isNumber == true)
    }

    @Test("Fake company from list")
    func fakeCompany() {
        let s = FakeStrategy(dataType: "company")
        let result = s.mask("Acme", context: ctx)!
        #expect(!result.isEmpty)
    }

    @Test("Fake city from list")
    func fakeCity() {
        let s = FakeStrategy(dataType: "city")
        let result = s.mask("NYC", context: ctx)!
        #expect(!result.isEmpty)
    }

    @Test("Infers type from column name")
    func inferType() {
        let emailCtx = MaskingContext(table: "t", column: "email")
        let s = FakeStrategy()
        let result = s.mask("john@example.com", context: emailCtx)!
        #expect(result.contains("@"))
    }

    @Test("Text type for unknown column")
    func textFallback() {
        let unknownCtx = MaskingContext(table: "t", column: "notes")
        let s = FakeStrategy()
        let result = s.mask("some text here", context: unknownCtx)!
        #expect(!result.isEmpty)
    }

    @Test("Deterministic")
    func deterministic() {
        let s = FakeStrategy(dataType: "name")
        let a = s.mask("John", context: ctx)
        let b = s.mask("John", context: ctx)
        #expect(a == b)
    }

    @Test("Options constructor")
    func options() {
        let s = FakeStrategy(options: ["type": "email", "locale": "en"])
        #expect(s.dataType == "email")
        #expect(s.locale == "en")
    }

    @Test("Strategy name")
    func name() {
        #expect(FakeStrategy.name == "fake")
    }

    @Test("First name type")
    func firstName() {
        let s = FakeStrategy(dataType: "first_name")
        let result = s.mask("John", context: ctx)!
        #expect(FakeStrategy.firstNames.contains(result))
    }

    @Test("Last name type")
    func lastName() {
        let s = FakeStrategy(dataType: "last_name")
        let result = s.mask("Doe", context: ctx)!
        #expect(FakeStrategy.lastNames.contains(result))
    }
}
