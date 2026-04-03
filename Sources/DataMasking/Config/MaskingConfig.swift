import Foundation

/// Top-level masking configuration.
///
/// Can be constructed programmatically or loaded from YAML via `MaskingConfigLoader`.
public struct MaskingConfig: Sendable, Codable, Equatable {
    /// Masking rules in evaluation order.
    public var rules: [MaskingRule]
    /// Global defaults.
    public var defaults: MaskingDefaults?

    public init(rules: [MaskingRule] = [], defaults: MaskingDefaults? = nil) {
        self.rules = rules
        self.defaults = defaults
    }

    // MARK: - Builder API

    /// Add a table-level rule with per-column strategies.
    @discardableResult
    public mutating func addTableRule(
        table: String,
        columns: [String: ColumnMaskConfig]
    ) -> Self {
        rules.append(MaskingRule(table: table, columns: columns))
        return self
    }

    /// Add a pattern-based rule that applies to any matching table.column.
    @discardableResult
    public mutating func addPatternRule(
        pattern: String,
        strategy: String,
        options: [String: String] = [:]
    ) -> Self {
        rules.append(MaskingRule(pattern: pattern, strategy: strategy, options: options))
        return self
    }
}

/// A single masking rule — either table-specific or pattern-based.
public struct MaskingRule: Sendable, Codable, Equatable {
    /// Exact table name match (e.g., "users").
    public var table: String?
    /// Glob pattern match (e.g., "*.phone", "users.*").
    public var pattern: String?
    /// Per-column strategy configs (for table-level rules).
    public var columns: [String: ColumnMaskConfig]?
    /// Strategy name (for pattern-based rules).
    public var strategy: String?
    /// Strategy options (for pattern-based rules).
    public var options: [String: String]?

    /// Create a table-level rule.
    public init(table: String, columns: [String: ColumnMaskConfig]) {
        self.table = table
        self.columns = columns
    }

    /// Create a pattern-based rule.
    public init(pattern: String, strategy: String, options: [String: String] = [:]) {
        self.pattern = pattern
        self.strategy = strategy
        self.options = options.isEmpty ? nil : options
    }

    /// Memberwise init for Codable.
    public init(
        table: String? = nil,
        pattern: String? = nil,
        columns: [String: ColumnMaskConfig]? = nil,
        strategy: String? = nil,
        options: [String: String]? = nil
    ) {
        self.table = table
        self.pattern = pattern
        self.columns = columns
        self.strategy = strategy
        self.options = options
    }
}

/// Configuration for a single column's masking strategy.
public struct ColumnMaskConfig: Sendable, Codable, Equatable {
    /// Strategy name (e.g., "hash", "redact", "partial").
    public var strategy: String
    /// Strategy-specific options.
    public var options: [String: String]?
    /// Optional DSL expression (overrides strategy if present).
    public var expression: String?

    public init(strategy: String, options: [String: String]? = nil, expression: String? = nil) {
        self.strategy = strategy
        self.options = options
        self.expression = expression
    }

    /// Convenience: create from just a strategy name.
    public static func strategy(_ name: String) -> ColumnMaskConfig {
        ColumnMaskConfig(strategy: name)
    }

    /// Convenience: create with strategy and options.
    public static func strategy(_ name: String, options: [String: String]) -> ColumnMaskConfig {
        ColumnMaskConfig(strategy: name, options: options)
    }

    /// Convenience: create from a DSL expression.
    public static func expression(_ expr: String) -> ColumnMaskConfig {
        ColumnMaskConfig(strategy: "dsl", expression: expr)
    }
}

/// Global masking defaults.
public struct MaskingDefaults: Sendable, Codable, Equatable {
    /// Seed for deterministic strategies. Defaults to 0.
    public var seed: UInt64?
    /// Locale hint for fake data generation.
    public var locale: String?
    /// Default strategy applied to unmatched columns (nil = passthrough).
    public var defaultStrategy: String?

    public init(seed: UInt64? = nil, locale: String? = nil, defaultStrategy: String? = nil) {
        self.seed = seed
        self.locale = locale
        self.defaultStrategy = defaultStrategy
    }
}
