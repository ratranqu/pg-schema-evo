import Testing
@testable import DataMasking

@Suite("RuleExpressionEvaluator Tests")
struct RuleExpressionEvaluatorTests {
    let evaluator = RuleExpressionEvaluator()
    let ctx = MaskingContext(table: "users", column: "email", seed: 42)

    @Test("Evaluate input returns raw value")
    func evalInput() throws {
        let result = try evaluator.evaluate(.input, value: "hello", context: ctx)
        #expect(result == "hello")
    }

    @Test("Evaluate string literal")
    func evalStringLiteral() throws {
        let result = try evaluator.evaluate(.stringLiteral("world"), value: "x", context: ctx)
        #expect(result == "world")
    }

    @Test("Evaluate int literal")
    func evalIntLiteral() throws {
        let result = try evaluator.evaluate(.intLiteral(42), value: "x", context: ctx)
        #expect(result == "42")
    }

    @Test("Evaluate email.local field access")
    func evalEmailLocal() throws {
        let result = try evaluator.evaluate(
            .fieldAccess(type: "email", field: "local"),
            value: "john@example.com",
            context: ctx
        )
        #expect(result == "john")
    }

    @Test("Evaluate email.domain field access")
    func evalEmailDomain() throws {
        let result = try evaluator.evaluate(
            .fieldAccess(type: "email", field: "domain"),
            value: "john@example.com",
            context: ctx
        )
        #expect(result == "example.com")
    }

    @Test("Evaluate hash function call")
    func evalHash() throws {
        let expr = RuleExpression.call(name: "hash", args: [.positional(.input)])
        let result = try evaluator.evaluate(expr, value: "test", context: ctx)
        #expect(result != nil)
        #expect(result != "test")
    }

    @Test("Evaluate concat")
    func evalConcat() throws {
        let expr = RuleExpression.concat([
            .fieldAccess(type: "email", field: "local"),
            .stringLiteral("@"),
            .stringLiteral("masked.com"),
        ])
        let result = try evaluator.evaluate(expr, value: "john@example.com", context: ctx)
        #expect(result == "john@masked.com")
    }

    @Test("Evaluate hash(email.local) + @ + email.domain")
    func evalComplexExpression() throws {
        let expr = RuleExpression.concat([
            .call(name: "hash", args: [.positional(.fieldAccess(type: "email", field: "local"))]),
            .stringLiteral("@"),
            .fieldAccess(type: "email", field: "domain"),
        ])
        let result = try evaluator.evaluate(expr, value: "john@example.com", context: ctx)!
        #expect(result.contains("@"))
        #expect(result.hasSuffix("example.com"))
        #expect(!result.hasPrefix("john@"))
    }

    @Test("Evaluate null call returns nil")
    func evalNull() throws {
        let expr = RuleExpression.call(name: "null", args: [.positional(.input)])
        let result = try evaluator.evaluate(expr, value: "test", context: ctx)
        #expect(result == nil)
    }

    @Test("NULL propagation in concat")
    func nullPropagation() throws {
        let expr = RuleExpression.concat([
            .call(name: "null", args: [.positional(.input)]),
            .stringLiteral("@test"),
        ])
        let result = try evaluator.evaluate(expr, value: "x", context: ctx)
        #expect(result == nil)
    }

    @Test("Error on unknown value type")
    func unknownType() {
        let expr = RuleExpression.fieldAccess(type: "unknown", field: "x")
        #expect(throws: MaskingError.self) {
            try evaluator.evaluate(expr, value: "test", context: ctx)
        }
    }

    @Test("Error on invalid email parse")
    func invalidEmailParse() {
        let expr = RuleExpression.fieldAccess(type: "email", field: "local")
        #expect(throws: MaskingError.self) {
            try evaluator.evaluate(expr, value: "notanemail", context: ctx)
        }
    }

    @Test("SSN field access")
    func ssnField() throws {
        let result = try evaluator.evaluate(
            .fieldAccess(type: "ssn", field: "serial"),
            value: "123-45-6789",
            context: ctx
        )
        #expect(result == "6789")
    }

    @Test("IP field access by index")
    func ipField() throws {
        let result = try evaluator.evaluate(
            .fieldAccess(type: "ip", field: "2"),
            value: "192.168.1.1",
            context: ctx
        )
        #expect(result == "1")
    }

    @Test("Credit card field access")
    func ccField() throws {
        let result = try evaluator.evaluate(
            .fieldAccess(type: "cc", field: "last"),
            value: "4111-1111-1111-9999",
            context: ctx
        )
        #expect(result == "9999")
    }

    @Test("Phone field access by index")
    func phoneField() throws {
        let result = try evaluator.evaluate(
            .fieldAccess(type: "phone", field: "0"),
            value: "555-123-4567",
            context: ctx
        )
        #expect(result == "555")
    }

    @Test("Function call with no args uses raw value")
    func callNoArgs() throws {
        let expr = RuleExpression.call(name: "redact", args: [])
        let result = try evaluator.evaluate(expr, value: "secret", context: ctx)
        #expect(result == "***")
    }

    @Test("Named args passed as options")
    func namedArgs() throws {
        let expr = RuleExpression.call(name: "redact", args: [
            .positional(.input),
            .named("value", .stringLiteral("HIDDEN")),
        ])
        let result = try evaluator.evaluate(expr, value: "secret", context: ctx)
        #expect(result == "HIDDEN")
    }
}
