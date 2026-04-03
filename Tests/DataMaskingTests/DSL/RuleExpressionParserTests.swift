import Testing
@testable import DataMasking

@Suite("RuleExpressionParser Tests")
struct RuleExpressionParserTests {
    let parser = RuleExpressionParser()

    @Test("Parse string literal")
    func stringLiteral() throws {
        let expr = try parser.parse("\"hello\"")
        #expect(expr == .stringLiteral("hello"))
    }

    @Test("Parse integer literal")
    func intLiteral() throws {
        let expr = try parser.parse("42")
        #expect(expr == .intLiteral(42))
    }

    @Test("Parse input keyword")
    func inputKeyword() throws {
        let expr = try parser.parse("input")
        #expect(expr == .input)
    }

    @Test("Parse field access")
    func fieldAccess() throws {
        let expr = try parser.parse("email.local")
        #expect(expr == .fieldAccess(type: "email", field: "local"))
    }

    @Test("Parse simple function call")
    func simpleCall() throws {
        let expr = try parser.parse("hash(input)")
        #expect(expr == .call(name: "hash", args: [.positional(.input)]))
    }

    @Test("Parse function call with field access arg")
    func callWithFieldAccess() throws {
        let expr = try parser.parse("hash(email.local)")
        #expect(expr == .call(name: "hash", args: [.positional(.fieldAccess(type: "email", field: "local"))]))
    }

    @Test("Parse function call with named arg")
    func callWithNamedArg() throws {
        let expr = try parser.parse("partial(input, keep: 3)")
        #expect(expr == .call(name: "partial", args: [
            .positional(.input),
            .named("keep", .intLiteral(3)),
        ]))
    }

    @Test("Parse concatenation")
    func concat() throws {
        let expr = try parser.parse("\"a\" + \"b\"")
        #expect(expr == .concat([.stringLiteral("a"), .stringLiteral("b")]))
    }

    @Test("Parse complex expression: hash(email.local) + @ + email.domain")
    func complexExpression() throws {
        let expr = try parser.parse("hash(email.local) + \"@\" + email.domain")
        #expect(expr == .concat([
            .call(name: "hash", args: [.positional(.fieldAccess(type: "email", field: "local"))]),
            .stringLiteral("@"),
            .fieldAccess(type: "email", field: "domain"),
        ]))
    }

    @Test("Parse function with string arg")
    func callWithStringArg() throws {
        let expr = try parser.parse("fake(\"email\")")
        #expect(expr == .call(name: "fake", args: [.positional(.stringLiteral("email"))]))
    }

    @Test("Parse nested function calls")
    func nestedCalls() throws {
        let expr = try parser.parse("hash(partial(input, keep: 1))")
        let inner = RuleExpression.call(name: "partial", args: [
            .positional(.input),
            .named("keep", .intLiteral(1)),
        ])
        #expect(expr == .call(name: "hash", args: [.positional(inner)]))
    }

    @Test("Parse parenthesized expression")
    func parenthesized() throws {
        let expr = try parser.parse("(\"a\" + \"b\")")
        #expect(expr == .concat([.stringLiteral("a"), .stringLiteral("b")]))
    }

    @Test("Parse bare identifier as field access on input")
    func bareIdentifier() throws {
        let expr = try parser.parse("name")
        #expect(expr == .fieldAccess(type: "input", field: "name"))
    }

    @Test("Parse with extra whitespace")
    func whitespace() throws {
        let expr = try parser.parse("  hash(  email.local  )  +  \"@\"  ")
        #expect(expr == .concat([
            .call(name: "hash", args: [.positional(.fieldAccess(type: "email", field: "local"))]),
            .stringLiteral("@"),
        ]))
    }

    @Test("Error on empty input")
    func emptyInput() {
        #expect(throws: MaskingError.self) {
            try parser.parse("")
        }
    }

    @Test("Error on invalid characters")
    func invalidChars() {
        #expect(throws: MaskingError.self) {
            try parser.parse("!!!")
        }
    }

    @Test("Error on unclosed function call")
    func unclosedCall() {
        #expect(throws: MaskingError.self) {
            try parser.parse("hash(input")
        }
    }

    @Test("No-arg function call")
    func noArgCall() throws {
        let expr = try parser.parse("null()")
        #expect(expr == .call(name: "null", args: []))
    }

    @Test("Multiple named args")
    func multipleNamedArgs() throws {
        let expr = try parser.parse("fake(input, type: \"email\", locale: \"en\")")
        if case .call(let name, let args) = expr {
            #expect(name == "fake")
            #expect(args.count == 3)
        } else {
            #expect(Bool(false), "Expected function call")
        }
    }
}
