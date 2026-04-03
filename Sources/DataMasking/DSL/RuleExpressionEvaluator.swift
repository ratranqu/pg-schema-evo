/// Evaluates a `RuleExpression` AST against an input value to produce masked output.
public struct RuleExpressionEvaluator: Sendable {
    private let registry: StrategyRegistry

    public init(registry: StrategyRegistry = StrategyRegistry()) {
        self.registry = registry
    }

    /// Evaluate an expression against an input value.
    /// - Parameters:
    ///   - expression: The parsed DSL expression.
    ///   - value: The raw input value to mask.
    ///   - context: Masking context for the current column.
    /// - Returns: The masked value, or nil for NULL.
    public func evaluate(
        _ expression: RuleExpression,
        value: String,
        context: MaskingContext
    ) throws -> String? {
        switch expression {
        case .input:
            return value

        case .stringLiteral(let s):
            return s

        case .intLiteral(let n):
            return String(n)

        case .fieldAccess(let type, let field):
            return try resolveFieldAccess(type: type, field: field, value: value)

        case .call(let name, let args):
            return try evaluateCall(name: name, args: args, value: value, context: context)

        case .concat(let exprs):
            var result = ""
            for expr in exprs {
                guard let part = try evaluate(expr, value: value, context: context) else {
                    return nil // NULL propagation
                }
                result += part
            }
            return result
        }
    }

    // MARK: - Field Access

    private func resolveFieldAccess(type: String, field: String, value: String) throws -> String? {
        switch type {
        case "email":
            let parser = EmailParser()
            guard let parts = parser.parse(value) else {
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Could not parse value as email: '\(value)'"
                )
            }
            switch field {
            case "local": return parts.local
            case "domain": return parts.domain
            default:
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Unknown email field '\(field)'. Valid fields: local, domain"
                )
            }

        case "phone":
            let parser = PhoneParser()
            guard let parts = parser.parse(value) else {
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Could not parse value as phone: '\(value)'"
                )
            }
            switch field {
            case "full": return parser.print(parts)
            default:
                if let idx = Int(field), idx >= 0, idx < parts.segments.count {
                    return parts.segments[idx]
                }
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Unknown phone field '\(field)'. Use numeric index (0, 1, ...) or 'full'"
                )
            }

        case "ip":
            let parser = IPAddressParser()
            guard let parts = parser.parse(value) else {
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Could not parse value as IP: '\(value)'"
                )
            }
            if let idx = Int(field), idx >= 0, idx < parts.octets.count {
                return parts.octets[idx]
            }
            throw MaskingError.dslEvalFailed(
                expression: "\(type).\(field)",
                detail: "Unknown IP field '\(field)'. Use numeric index (0-3)"
            )

        case "ssn":
            let parser = SSNParser()
            guard let parts = parser.parse(value) else {
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Could not parse value as SSN: '\(value)'"
                )
            }
            switch field {
            case "area": return parts.area
            case "group": return parts.group
            case "serial": return parts.serial
            default:
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Unknown SSN field '\(field)'. Valid fields: area, group, serial"
                )
            }

        case "cc", "card":
            let parser = CreditCardParser()
            guard let parts = parser.parse(value) else {
                throw MaskingError.dslEvalFailed(
                    expression: "\(type).\(field)",
                    detail: "Could not parse value as credit card: '\(value)'"
                )
            }
            if let idx = Int(field), idx >= 0, idx < parts.groups.count {
                return parts.groups[idx]
            }
            if field == "last" {
                return parts.groups.last
            }
            throw MaskingError.dslEvalFailed(
                expression: "\(type).\(field)",
                detail: "Unknown card field '\(field)'. Use numeric index or 'last'"
            )

        case "input":
            // Bare field name — return the raw value
            return value

        default:
            throw MaskingError.dslEvalFailed(
                expression: "\(type).\(field)",
                detail: "Unknown value type '\(type)'. Supported: email, phone, ip, ssn, cc"
            )
        }
    }

    // MARK: - Function Calls

    private func evaluateCall(
        name: String,
        args: [RuleArg],
        value: String,
        context: MaskingContext
    ) throws -> String? {
        // Extract the first positional arg as the input expression, or use raw value
        let inputValue: String?
        var optionArgs: [String: String] = context.options

        if let firstArg = args.first, case .positional(let expr) = firstArg {
            inputValue = try evaluate(expr, value: value, context: context)
        } else {
            inputValue = value
        }

        // Collect named args into options
        for arg in args {
            if case .named(let key, let valExpr) = arg {
                if let v = try evaluate(valExpr, value: value, context: context) {
                    optionArgs[key] = v
                }
            }
        }

        guard let inputValue else { return nil }

        // Create strategy and apply
        let strategy = try registry.create(name: name, options: optionArgs)
        let ctx = MaskingContext(
            table: context.table,
            column: context.column,
            options: optionArgs,
            seed: context.seed
        )
        return strategy.mask(inputValue, context: ctx)
    }
}
