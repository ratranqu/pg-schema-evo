import Testing
@testable import DataMasking

@Suite("MaskingEngine Tests")
struct MaskingEngineTests {
    @Test("Table rule masks specified columns")
    func tableRule() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "users", columns: [
            "email": .strategy("hash"),
            "name": .strategy("redact"),
        ])
        let engine = try MaskingEngine(config: config)

        let result = engine.maskRow(
            table: "users",
            columns: ["id", "email", "name"],
            values: ["1", "john@example.com", "John Doe"]
        )
        #expect(result[0] == "1") // passthrough
        #expect(result[1] != "john@example.com") // hashed
        #expect(result[2] == "***") // redacted
    }

    @Test("Pattern rule matches across tables")
    func patternRule() throws {
        var config = MaskingConfig()
        config.addPatternRule(pattern: "*.email", strategy: "hash")
        let engine = try MaskingEngine(config: config)

        let r1 = engine.mask(value: "john@test.com", table: "users", column: "email")
        let r2 = engine.mask(value: "jane@test.com", table: "orders", column: "email")
        #expect(r1 != "john@test.com")
        #expect(r2 != "jane@test.com")
    }

    @Test("Unmatched columns pass through")
    func passthrough() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "users", columns: [
            "email": .strategy("hash"),
        ])
        let engine = try MaskingEngine(config: config)

        let result = engine.mask(value: "keep me", table: "users", column: "id")
        #expect(result == "keep me")
    }

    @Test("NULL values pass through")
    func nullPassthrough() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "t", columns: ["c": .strategy("hash")])
        let engine = try MaskingEngine(config: config)

        let result = engine.maskRow(
            table: "t",
            columns: ["c"],
            values: [nil]
        )
        #expect(result[0] == nil)
    }

    @Test("Table rules take precedence over pattern rules")
    func precedence() throws {
        var config = MaskingConfig()
        config.addPatternRule(pattern: "*.email", strategy: "redact")
        config.addTableRule(table: "users", columns: [
            "email": .strategy("hash"),
        ])
        let engine = try MaskingEngine(config: config)

        let result = engine.mask(value: "john@example.com", table: "users", column: "email")
        // Table rule (hash) should win over pattern rule (redact)
        #expect(result != "***") // not redacted
        #expect(result != "john@example.com") // is masked
    }

    @Test("Default strategy applies to unmatched")
    func defaultStrategy() throws {
        var config = MaskingConfig(defaults: MaskingDefaults(defaultStrategy: "redact"))
        let engine = try MaskingEngine(config: config)

        let result = engine.mask(value: "secret", table: "anything", column: "any")
        #expect(result == "***")
    }

    @Test("DSL expression column config")
    func dslExpression() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "users", columns: [
            "email": .expression("hash(email.local) + \"@\" + email.domain"),
        ])
        let engine = try MaskingEngine(config: config)

        let result = engine.mask(value: "john@example.com", table: "users", column: "email")!
        #expect(result.contains("@"))
        #expect(result.hasSuffix("example.com"))
        #expect(!result.hasPrefix("john@"))
    }

    @Test("Custom strategy registration")
    func customStrategy() throws {
        struct UpperStrategy: MaskingStrategy {
            static let name = "upper"
            func mask(_ value: String, context: MaskingContext) -> String? {
                value.uppercased()
            }
        }

        var registry = StrategyRegistry()
        registry.register(name: "upper") { _ in UpperStrategy() }

        var config = MaskingConfig()
        config.addTableRule(table: "t", columns: ["c": .strategy("upper")])
        let engine = try MaskingEngine(config: config, registry: registry)

        #expect(engine.mask(value: "hello", table: "t", column: "c") == "HELLO")
    }

    @Test("Multiple columns masked correctly")
    func multipleColumns() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "users", columns: [
            "email": .strategy("hash"),
            "name": .strategy("fake"),
            "ssn": .strategy("redact", options: ["value": "XXX-XX-XXXX"]),
            "phone": .strategy("partial", options: ["type": "phone"]),
        ])
        let engine = try MaskingEngine(config: config)

        let result = engine.maskRow(
            table: "users",
            columns: ["id", "email", "name", "ssn", "phone"],
            values: ["1", "john@example.com", "John Doe", "123-45-6789", "555-123-4567"]
        )
        #expect(result[0] == "1") // passthrough
        #expect(result[1] != "john@example.com") // hashed
        #expect(result[2] != "John Doe") // faked
        #expect(result[3] == "XXX-XX-XXXX") // redacted
        #expect(result[4] != "555-123-4567") // partial
    }

    @Test("Resolve returns nil for unmatched column")
    func resolveNil() throws {
        let engine = try MaskingEngine(config: MaskingConfig())
        #expect(engine.resolve(table: "t", column: "c") == nil)
    }

    @Test("Resolve returns masker for matched column")
    func resolveMatch() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "t", columns: ["c": .strategy("hash")])
        let engine = try MaskingEngine(config: config)
        #expect(engine.resolve(table: "t", column: "c") != nil)
    }

    @Test("maskRow with no matching rules returns original values")
    func maskRowNoRules() throws {
        let engine = try MaskingEngine(config: MaskingConfig())
        let values: [String?] = ["a", "b", nil, "d"]
        let result = engine.maskRow(table: "t", columns: ["c1", "c2", "c3", "c4"], values: values)
        #expect(result == values)
    }

    @Test("Null strategy via engine")
    func nullStrategy() throws {
        var config = MaskingConfig()
        config.addTableRule(table: "t", columns: ["c": .strategy("null")])
        let engine = try MaskingEngine(config: config)
        #expect(engine.mask(value: "hello", table: "t", column: "c") == nil)
    }
}

@Suite("StrategyRegistry Tests")
struct StrategyRegistryTests {
    @Test("All built-in strategies are registered")
    func builtins() {
        let registry = StrategyRegistry()
        let names = registry.registeredNames
        #expect(names.contains("null"))
        #expect(names.contains("redact"))
        #expect(names.contains("hash"))
        #expect(names.contains("partial"))
        #expect(names.contains("preserve-format"))
        #expect(names.contains("fake"))
        #expect(names.contains("regex"))
        #expect(names.contains("numeric-noise"))
    }

    @Test("Create known strategy")
    func createKnown() throws {
        let registry = StrategyRegistry()
        let s = try registry.create(name: "redact")
        #expect(type(of: s) is RedactStrategy.Type)
    }

    @Test("Create unknown throws")
    func createUnknown() {
        let registry = StrategyRegistry()
        #expect(throws: MaskingError.self) {
            try registry.create(name: "nonexistent")
        }
    }

    @Test("Custom registration")
    func custom() throws {
        struct Custom: MaskingStrategy {
            static let name = "custom"
            func mask(_ value: String, context: MaskingContext) -> String? { "custom" }
        }
        var registry = StrategyRegistry()
        registry.register(name: "custom") { _ in Custom() }
        let s = try registry.create(name: "custom")
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("x", context: ctx) == "custom")
    }

    @Test("Override built-in")
    func override() throws {
        struct MyHash: MaskingStrategy {
            static let name = "hash"
            func mask(_ value: String, context: MaskingContext) -> String? { "overridden" }
        }
        var registry = StrategyRegistry()
        registry.register(name: "hash") { _ in MyHash() }
        let s = try registry.create(name: "hash")
        let ctx = MaskingContext(table: "t", column: "c")
        #expect(s.mask("x", context: ctx) == "overridden")
    }
}
