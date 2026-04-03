import Testing
@testable import DataMasking

@Suite("HashStrategy Tests")
struct HashStrategyTests {
    let ctx = MaskingContext(table: "users", column: "email", seed: 42)

    @Test("Deterministic - same input produces same output")
    func deterministic() {
        let s = HashStrategy()
        let a = s.mask("john@example.com", context: ctx)
        let b = s.mask("john@example.com", context: ctx)
        #expect(a == b)
    }

    @Test("Different inputs produce different outputs")
    func different() {
        let s = HashStrategy()
        let a = s.mask("john@example.com", context: ctx)
        let b = s.mask("jane@example.com", context: ctx)
        #expect(a != b)
    }

    @Test("Default output length is 16")
    func defaultLength() {
        let s = HashStrategy()
        let result = s.mask("test", context: ctx)!
        #expect(result.count == 16)
    }

    @Test("Custom length")
    func customLength() {
        let s = HashStrategy(length: 8)
        let result = s.mask("test", context: ctx)!
        #expect(result.count == 8)
    }

    @Test("Long output uses two hashes")
    func longOutput() {
        let s = HashStrategy(length: 24)
        let result = s.mask("test", context: ctx)!
        #expect(result.count == 24)
    }

    @Test("Prefix is prepended")
    func prefix() {
        let s = HashStrategy(length: 8, prefix: "usr_")
        let result = s.mask("test", context: ctx)!
        #expect(result.hasPrefix("usr_"))
        #expect(result.count == 12) // 4 prefix + 8 hash
    }

    @Test("Salt changes output")
    func salt() {
        let a = HashStrategy(salt: "salt1").mask("test", context: ctx)
        let b = HashStrategy(salt: "salt2").mask("test", context: ctx)
        #expect(a != b)
    }

    @Test("Options constructor")
    func options() {
        let s = HashStrategy(options: ["length": "10", "prefix": "x", "salt": "s"])
        #expect(s.length == 10)
        #expect(s.prefix == "x")
        #expect(s.salt == "s")
    }

    @Test("Output is valid hex")
    func hexOutput() {
        let s = HashStrategy()
        let result = s.mask("anything", context: ctx)!
        let hexChars = Set("0123456789abcdef")
        #expect(result.allSatisfy { hexChars.contains($0) })
    }

    @Test("Strategy name is hash")
    func name() {
        #expect(HashStrategy.name == "hash")
    }

    @Test("Referential integrity: same value, different columns, same hash")
    func referentialIntegrity() {
        let s = HashStrategy()
        // Same value with same salt produces same hash regardless of column context
        let ctx1 = MaskingContext(table: "orders", column: "user_email")
        let ctx2 = MaskingContext(table: "users", column: "email")
        let a = s.mask("john@example.com", context: ctx1)
        let b = s.mask("john@example.com", context: ctx2)
        #expect(a == b)
    }
}
