/// Parses masking DSL expression strings into `RuleExpression` ASTs.
///
/// Uses a hand-rolled recursive descent parser for Swift 6.2 strict concurrency
/// compatibility (swift-parsing types are not `Sendable`).
///
/// Grammar:
/// ```
/// expr       = concat
/// concat     = atom ("+" atom)*
/// atom       = call | fieldAccess | stringLiteral | intLiteral | "input" | "(" expr ")"
/// call       = IDENT "(" arglist? ")"
/// arglist    = arg ("," arg)*
/// arg        = IDENT ":" expr | expr
/// fieldAccess = IDENT "." IDENT
/// IDENT      = [a-zA-Z_][a-zA-Z0-9_]*
/// stringLiteral = '"' [^"]* '"'
/// intLiteral = [0-9]+
/// ```
///
/// Examples:
/// - `hash(email.local) + "@" + email.domain`
/// - `partial(name, keep: 1)`
/// - `"REDACTED"`
public struct RuleExpressionParser: Sendable {
    public init() {}

    /// Parse a DSL expression string into an AST.
    public func parse(_ input: String) throws -> RuleExpression {
        var cursor = Cursor(input)
        let result = try parseConcat(&cursor)
        cursor.skipWhitespace()
        guard cursor.isAtEnd else {
            throw MaskingError.dslParseFailed(
                expression: input,
                detail: "Unexpected characters: '\(cursor.remaining)'"
            )
        }
        return result
    }
}

// MARK: - Cursor

/// A simple string cursor for recursive descent parsing.
private struct Cursor {
    private let source: String
    private(set) var position: String.Index

    init(_ string: String) {
        self.source = string
        self.position = string.startIndex
    }

    var isAtEnd: Bool { position >= source.endIndex }

    var peek: Character? {
        isAtEnd ? nil : source[position]
    }

    var remaining: Substring {
        source[position...]
    }

    @discardableResult
    mutating func advance() -> Character? {
        guard !isAtEnd else { return nil }
        let ch = source[position]
        position = source.index(after: position)
        return ch
    }

    mutating func skipWhitespace() {
        while let ch = peek, ch.isWhitespace {
            advance()
        }
    }

    mutating func tryConsume(_ char: Character) -> Bool {
        if peek == char {
            advance()
            return true
        }
        return false
    }

    mutating func parseIdentifier() -> String? {
        skipWhitespace()
        guard let first = peek, first.isLetter || first == "_" else { return nil }
        let start = position
        while let ch = peek, ch.isLetter || ch.isNumber || ch == "_" {
            advance()
        }
        return String(source[start..<position])
    }

    mutating func parseStringLiteral() -> String? {
        skipWhitespace()
        guard peek == "\"" else { return nil }
        advance() // consume opening quote
        let start = position
        while let ch = peek, ch != "\"" {
            advance()
        }
        let content = String(source[start..<position])
        advance() // consume closing quote
        return content
    }

    mutating func parseIntLiteral() -> Int? {
        skipWhitespace()
        guard let first = peek, first.isNumber else { return nil }
        let start = position
        while let ch = peek, ch.isNumber {
            advance()
        }
        return Int(source[start..<position])
    }

    var savedPosition: String.Index {
        get { position }
        set { position = newValue }
    }
}

// MARK: - Parsing Functions

private func parseConcat(_ cursor: inout Cursor) throws -> RuleExpression {
    var terms: [RuleExpression] = []
    let first = try parseAtom(&cursor)
    terms.append(first)

    while true {
        cursor.skipWhitespace()
        guard cursor.tryConsume("+") else { break }
        cursor.skipWhitespace()
        let next = try parseAtom(&cursor)
        terms.append(next)
    }

    return terms.count == 1 ? terms[0] : .concat(terms)
}

private func parseAtom(_ cursor: inout Cursor) throws -> RuleExpression {
    cursor.skipWhitespace()

    // String literal
    if cursor.peek == "\"" {
        guard let str = cursor.parseStringLiteral() else {
            throw MaskingError.dslParseFailed(expression: "", detail: "Unterminated string literal")
        }
        return .stringLiteral(str)
    }

    // Parenthesized expression
    if cursor.tryConsume("(") {
        let expr = try parseConcat(&cursor)
        cursor.skipWhitespace()
        guard cursor.tryConsume(")") else {
            throw MaskingError.dslParseFailed(expression: "", detail: "Expected ')'")
        }
        return expr
    }

    // Identifier-based: function call, field access, "input" keyword
    if let ident = cursor.parseIdentifier() {
        // Function call: ident(...)
        cursor.skipWhitespace()
        if cursor.tryConsume("(") {
            var args: [RuleArg] = []
            cursor.skipWhitespace()
            if cursor.peek != ")" {
                let arg = try parseArg(&cursor)
                args.append(arg)
                while true {
                    cursor.skipWhitespace()
                    guard cursor.tryConsume(",") else { break }
                    let arg = try parseArg(&cursor)
                    args.append(arg)
                }
            }
            cursor.skipWhitespace()
            guard cursor.tryConsume(")") else {
                throw MaskingError.dslParseFailed(expression: ident, detail: "Expected ')' in function call")
            }
            return .call(name: ident, args: args)
        }

        // Field access: ident.ident
        if cursor.tryConsume(".") {
            guard let field = cursor.parseIdentifier() else {
                throw MaskingError.dslParseFailed(expression: ident, detail: "Expected field name after '.'")
            }
            return .fieldAccess(type: ident, field: field)
        }

        // "input" keyword
        if ident == "input" {
            return .input
        }

        // Bare identifier
        return .fieldAccess(type: "input", field: ident)
    }

    // Integer literal
    if let n = cursor.parseIntLiteral() {
        return .intLiteral(n)
    }

    throw MaskingError.dslParseFailed(
        expression: String(cursor.remaining.prefix(20)),
        detail: "Expected expression"
    )
}

private func parseArg(_ cursor: inout Cursor) throws -> RuleArg {
    cursor.skipWhitespace()
    // Try named arg: ident: expr
    let saved = cursor.savedPosition
    if let ident = cursor.parseIdentifier() {
        cursor.skipWhitespace()
        if cursor.tryConsume(":") {
            let expr = try parseConcat(&cursor)
            return .named(ident, expr)
        }
        // Not a named arg, restore
        cursor.savedPosition = saved
    }
    // Positional arg
    let expr = try parseConcat(&cursor)
    return .positional(expr)
}
