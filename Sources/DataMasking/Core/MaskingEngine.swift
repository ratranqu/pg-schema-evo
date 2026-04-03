import Foundation

/// The main entry point for applying masking rules to tabular data.
///
/// Pure Swift, no database dependencies. Can be used standalone.
///
/// Usage:
/// ```swift
/// var config = MaskingConfig()
/// config.addTableRule(table: "users", columns: [
///     "email": .strategy("hash"),
///     "name": .strategy("fake"),
///     "ssn": .strategy("redact", options: ["value": "XXX-XX-XXXX"]),
/// ])
///
/// let engine = try MaskingEngine(config: config)
/// let masked = engine.maskRow(
///     table: "users",
///     columns: ["id", "email", "name", "ssn"],
///     values: ["1", "john@example.com", "John Doe", "123-45-6789"]
/// )
/// ```
public struct MaskingEngine: Sendable {
    private let config: MaskingConfig
    private let registry: StrategyRegistry
    private let matcher: PatternMatcher
    private let dslEvaluator: RuleExpressionEvaluator
    private let dslParser: RuleExpressionParser
    /// Pre-compiled rule lookup: [table: [column: ResolvedMasker]]
    private let compiled: [String: [String: ResolvedMasker]]

    /// Create a masking engine from a configuration.
    /// - Parameters:
    ///   - config: The masking configuration.
    ///   - registry: Strategy registry (defaults to built-in strategies).
    public init(config: MaskingConfig, registry: StrategyRegistry = StrategyRegistry()) throws {
        self.config = config
        self.registry = registry
        self.matcher = PatternMatcher()
        self.dslEvaluator = RuleExpressionEvaluator(registry: registry)
        self.dslParser = RuleExpressionParser()
        self.compiled = [:]  // Lazy compilation per table
    }

    /// Mask a single value for a given table and column.
    /// - Returns: The masked value, or nil for NULL. Returns the original value if no rule matches.
    public func mask(value: String, table: String, column: String) -> String? {
        guard let masker = resolve(table: table, column: column) else {
            return value // passthrough
        }
        let context = makeContext(table: table, column: column, options: masker.options)
        return masker.apply(value, context: context)
    }

    /// Mask an entire row of values for a given table.
    ///
    /// When no masking rule matches a column, the value passes through unchanged
    /// with zero additional allocations.
    /// - Parameters:
    ///   - table: The table name.
    ///   - columns: Ordered column names.
    ///   - values: Ordered column values (nil = SQL NULL).
    /// - Returns: Masked values in the same order.
    public func maskRow(
        table: String,
        columns: [String],
        values: [String?]
    ) -> [String?] {
        var result = values
        var changed = false
        for (i, col) in columns.enumerated() {
            guard let original = values[i] else { continue } // NULL passthrough
            guard let masker = resolve(table: table, column: col) else { continue }
            let context = makeContext(table: table, column: col, options: masker.options)
            let masked = masker.apply(original, context: context)
            if masked != original || masked == nil {
                if !changed {
                    changed = true
                }
                result[i] = masked
            }
        }
        return result
    }

    /// Resolve which masker applies to a specific table.column.
    /// Returns nil if no rule matches (value passes through).
    public func resolve(table: String, column: String) -> ResolvedMasker? {
        // Check table-specific rules first (higher priority)
        for rule in config.rules {
            if let ruleTable = rule.table, ruleTable == table {
                if let columns = rule.columns, let colConfig = columns[column] {
                    return resolvedMasker(from: colConfig)
                }
            }
        }

        // Check pattern-based rules
        for rule in config.rules {
            if let pattern = rule.pattern {
                if matcher.matches(pattern: pattern, table: table, column: column) {
                    if let strategyName = rule.strategy {
                        let options = rule.options ?? [:]
                        if let strategy = try? registry.create(name: strategyName, options: options) {
                            return ResolvedMasker(strategy: strategy, options: options)
                        }
                    }
                }
            }
        }

        // Check default strategy
        if let defaultName = config.defaults?.defaultStrategy {
            if let strategy = try? registry.create(name: defaultName) {
                return ResolvedMasker(strategy: strategy, options: [:])
            }
        }

        return nil
    }

    private func resolvedMasker(from colConfig: ColumnMaskConfig) -> ResolvedMasker? {
        // DSL expression takes priority
        if let expr = colConfig.expression {
            if let parsed = try? dslParser.parse(expr) {
                return ResolvedMasker(expression: parsed, evaluator: dslEvaluator, options: colConfig.options ?? [:])
            }
        }

        let options = colConfig.options ?? [:]
        if let strategy = try? registry.create(name: colConfig.strategy, options: options) {
            return ResolvedMasker(strategy: strategy, options: options)
        }
        return nil
    }

    private func makeContext(table: String, column: String, options: [String: String]) -> MaskingContext {
        let seed = config.defaults?.seed ?? fnv1a64("\(table).\(column)")
        return MaskingContext(table: table, column: column, options: options, seed: seed)
    }
}

/// A pre-resolved masker for a specific column.
///
/// Can be either strategy-based or DSL-expression-based.
public struct ResolvedMasker: Sendable {
    private enum Kind: Sendable {
        case strategy(any MaskingStrategy)
        case expression(RuleExpression, RuleExpressionEvaluator)
    }

    private let kind: Kind
    public let options: [String: String]

    init(strategy: any MaskingStrategy, options: [String: String]) {
        self.kind = .strategy(strategy)
        self.options = options
    }

    init(expression: RuleExpression, evaluator: RuleExpressionEvaluator, options: [String: String]) {
        self.kind = .expression(expression, evaluator)
        self.options = options
    }

    /// Apply the masker to a value.
    public func apply(_ value: String, context: MaskingContext) -> String? {
        switch kind {
        case .strategy(let strategy):
            return strategy.mask(value, context: context)
        case .expression(let expr, let evaluator):
            return try? evaluator.evaluate(expr, value: value, context: context)
        }
    }
}
