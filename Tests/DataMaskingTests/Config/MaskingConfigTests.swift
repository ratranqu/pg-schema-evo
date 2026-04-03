import Testing
@testable import DataMasking

@Suite("MaskingConfig Tests")
struct MaskingConfigTests {
    @Test("Programmatic config construction")
    func programmatic() {
        var config = MaskingConfig()
        config.addTableRule(table: "users", columns: [
            "email": .strategy("hash"),
            "name": .strategy("fake"),
        ])
        config.addPatternRule(pattern: "*.phone", strategy: "partial", options: ["keep": "3"])

        #expect(config.rules.count == 2)
        #expect(config.rules[0].table == "users")
        #expect(config.rules[0].columns?["email"]?.strategy == "hash")
        #expect(config.rules[1].pattern == "*.phone")
        #expect(config.rules[1].strategy == "partial")
    }

    @Test("ColumnMaskConfig convenience initializers")
    func columnMaskConvenience() {
        let a = ColumnMaskConfig.strategy("hash")
        #expect(a.strategy == "hash")
        #expect(a.options == nil)

        let b = ColumnMaskConfig.strategy("partial", options: ["keep": "2"])
        #expect(b.options?["keep"] == "2")

        let c = ColumnMaskConfig.expression("hash(email.local)")
        #expect(c.expression == "hash(email.local)")
    }

    @Test("MaskingDefaults")
    func defaults() {
        let d = MaskingDefaults(seed: 42, locale: "en", defaultStrategy: "redact")
        #expect(d.seed == 42)
        #expect(d.locale == "en")
        #expect(d.defaultStrategy == "redact")
    }

    @Test("Empty config")
    func emptyConfig() {
        let config = MaskingConfig()
        #expect(config.rules.isEmpty)
    }

    @Test("MaskingRule table init")
    func tableRule() {
        let rule = MaskingRule(table: "users", columns: ["email": .strategy("hash")])
        #expect(rule.table == "users")
        #expect(rule.pattern == nil)
    }

    @Test("MaskingRule pattern init")
    func patternRule() {
        let rule = MaskingRule(pattern: "*.ssn", strategy: "redact")
        #expect(rule.pattern == "*.ssn")
        #expect(rule.table == nil)
    }
}

@Suite("MaskingConfigLoader Tests")
struct MaskingConfigLoaderTests {
    let loader = MaskingConfigLoader()

    @Test("Load from YAML string")
    func loadFromString() throws {
        let yaml = """
        rules:
          - table: users
            columns:
              email:
                strategy: hash
              name:
                strategy: fake
          - pattern: "*.phone"
            strategy: partial
            options:
              keep: "3"
        defaults:
          seed: 42
          locale: en
        """
        let config = try loader.loadFromString(yaml)
        #expect(config.rules.count == 2)
        #expect(config.rules[0].table == "users")
        #expect(config.rules[0].columns?["email"]?.strategy == "hash")
        #expect(config.rules[1].pattern == "*.phone")
        #expect(config.defaults?.seed == 42)
        #expect(config.defaults?.locale == "en")
    }

    @Test("Invalid YAML throws config error")
    func invalidYaml() {
        let yaml = "{{invalid yaml"
        #expect(throws: MaskingError.self) {
            try loader.loadFromString(yaml)
        }
    }

    @Test("Missing file throws config error")
    func missingFile() {
        #expect(throws: MaskingError.self) {
            try loader.load(from: "/nonexistent/path/masking.yaml")
        }
    }

    @Test("Minimal YAML with just rules")
    func minimalYaml() throws {
        let yaml = """
        rules:
          - table: t
            columns:
              c:
                strategy: null
        """
        let config = try loader.loadFromString(yaml)
        #expect(config.rules.count == 1)
    }

    @Test("YAML with expression")
    func yamlWithExpression() throws {
        let yaml = """
        rules:
          - table: users
            columns:
              email:
                strategy: dsl
                expression: "hash(email.local) + \\"@\\" + email.domain"
        """
        let config = try loader.loadFromString(yaml)
        #expect(config.rules[0].columns?["email"]?.expression != nil)
    }
}

@Suite("PatternMatcher Tests")
struct PatternMatcherTests {
    let matcher = PatternMatcher()

    @Test("Wildcard table, exact column")
    func wildcardTableExactColumn() {
        #expect(matcher.matches(pattern: "*.email", table: "users", column: "email"))
        #expect(matcher.matches(pattern: "*.email", table: "orders", column: "email"))
        #expect(!matcher.matches(pattern: "*.email", table: "users", column: "name"))
    }

    @Test("Exact table, wildcard column")
    func exactTableWildcardColumn() {
        #expect(matcher.matches(pattern: "users.*", table: "users", column: "email"))
        #expect(matcher.matches(pattern: "users.*", table: "users", column: "name"))
        #expect(!matcher.matches(pattern: "users.*", table: "orders", column: "email"))
    }

    @Test("Exact table and column")
    func exactMatch() {
        #expect(matcher.matches(pattern: "users.email", table: "users", column: "email"))
        #expect(!matcher.matches(pattern: "users.email", table: "users", column: "name"))
    }

    @Test("Wildcard both")
    func wildcardBoth() {
        #expect(matcher.matches(pattern: "*.*", table: "any", column: "thing"))
    }

    @Test("Column-only pattern (no dot)")
    func columnOnly() {
        #expect(matcher.matches(pattern: "email", table: "any", column: "email"))
        #expect(!matcher.matches(pattern: "email", table: "any", column: "name"))
    }
}
