/// AST for the masking rule mini-language.
///
/// Grammar:
/// ```
/// expr       = concat
/// concat     = term ("+" term)*
/// term       = call | fieldAccess | literal | "(" expr ")"
/// call       = IDENT "(" arglist ")"
/// arglist    = (namedArg | expr) ("," (namedArg | expr))*
/// namedArg   = IDENT ":" expr
/// fieldAccess = IDENT "." IDENT
/// literal    = '"' [^"]* '"' | INT
/// ```
///
/// Examples:
/// - `hash(email.local) + "@" + email.domain`
/// - `partial(name, keep: 1)`
/// - `fake("email", locale: "en")`
public indirect enum RuleExpression: Sendable, Equatable {
    /// A strategy function call: `hash(expr)`, `partial(expr, keep: 1)`.
    case call(name: String, args: [RuleArg])
    /// A field access on a parsed value type: `email.local`, `phone.segments`.
    case fieldAccess(type: String, field: String)
    /// String concatenation: `expr + expr + ...`.
    case concat([RuleExpression])
    /// A string literal: `"@"`.
    case stringLiteral(String)
    /// An integer literal: `1`, `3`.
    case intLiteral(Int)
    /// The raw input value (implicit when no expression specified).
    case input
}

/// An argument in a function call — either positional or named.
public enum RuleArg: Sendable, Equatable {
    case positional(RuleExpression)
    case named(String, RuleExpression)
}
