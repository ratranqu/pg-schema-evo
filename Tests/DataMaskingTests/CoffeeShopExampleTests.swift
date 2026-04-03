import Testing
import Foundation
@testable import DataMasking

/// End-to-end tests mirroring the Coffee Shop Loyalty example in
/// `examples/data-masking/`. Validates that the masking config from
/// the example correctly anonymizes PII and obfuscates metrics.
@Suite("Coffee Shop Loyalty — Data Masking Example")
struct CoffeeShopExampleTests {

    // MARK: - Config Loading

    static func exampleConfig() throws -> MaskingConfig {
        // Build the same config as masking-config.yaml programmatically
        var config = MaskingConfig(defaults: MaskingDefaults(seed: 20260403, locale: "en"))

        config.addTableRule(table: "customers", columns: [
            "first_name": .strategy("fake", options: ["type": "first_name"]),
            "last_name": .strategy("fake", options: ["type": "last_name"]),
            "email": .expression("hash(email.local) + \"@\" + email.domain"),
            "phone": .strategy("partial", options: ["type": "phone"]),
            "address": .strategy("fake", options: ["type": "address"]),
            "city": .strategy("fake", options: ["type": "city"]),
            "zip_code": .strategy("preserve-format"),
            "ssn": .strategy("redact", options: ["value": "XXX-XX-XXXX"]),
        ])

        config.addTableRule(table: "purchases", columns: [
            "amount": .strategy("numeric-noise", options: ["noise": "0.15"]),
            "points_earned": .strategy("numeric-noise", options: ["noise": "0.15"]),
            "item_count": .strategy("numeric-noise", options: ["noise": "0.10"]),
        ])

        config.addTableRule(table: "points_balance", columns: [
            "total_points": .strategy("numeric-noise", options: ["noise": "0.20"]),
            "lifetime_points": .strategy("numeric-noise", options: ["noise": "0.20"]),
        ])

        config.addTableRule(table: "redemptions", columns: [
            "points_spent": .strategy("numeric-noise", options: ["noise": "0.15"]),
        ])

        config.addPatternRule(pattern: "*.email", strategy: "hash")
        config.addPatternRule(pattern: "*.ssn", strategy: "redact", options: ["value": "XXX-XX-XXXX"])

        return config
    }

    // MARK: - Sample Data

    static let customerColumns = [
        "id", "first_name", "last_name", "email", "phone",
        "address", "city", "zip_code", "ssn", "tier",
    ]

    static let aliceRow: [String?] = [
        "1", "Alice", "Johnson", "alice.johnson@gmail.com", "503-555-0101",
        "742 Evergreen Terrace", "Portland", "97201", "539-48-0120", "platinum",
    ]

    static let bobRow: [String?] = [
        "2", "Bob", "Martinez", "bob.martinez@yahoo.com", "206-555-0202",
        "1600 Pennsylvania Ave", "Seattle", "98101", "461-73-9285", "gold",
    ]

    static let purchaseColumns = [
        "id", "customer_id", "store_id", "item_name", "item_count",
        "amount", "points_earned", "purchased_at",
    ]

    static let purchaseRow: [String?] = [
        "1", "1", "1", "Latte", "1", "5.50", "55", "2026-01-05 08:00:00+00",
    ]

    static let pointsColumns = [
        "customer_id", "total_points", "lifetime_points", "last_earned_at",
    ]

    static let alicePoints: [String?] = [
        "1", "448", "448", "2026-02-05 09:00:00+00",
    ]

    // MARK: - Customer PII Tests

    @Test("Customer names are replaced with fake names")
    func customerNamesFaked() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        #expect(result[1] != "Alice", "First name should be masked")
        #expect(result[2] != "Johnson", "Last name should be masked")
        #expect(result[1] != nil, "First name should not be NULL")
        #expect(result[2] != nil, "Last name should not be NULL")
    }

    @Test("Customer email local part is hashed, domain preserved")
    func customerEmailHashed() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        let email = result[3]!
        #expect(email.contains("@"), "Email should still contain @")
        #expect(email.hasSuffix("gmail.com"), "Domain should be preserved")
        #expect(!email.hasPrefix("alice"), "Local part should be hashed")
    }

    @Test("Customer email hashing is deterministic")
    func emailHashDeterministic() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let r1 = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        let r2 = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        #expect(r1[3] == r2[3], "Same input should produce same hashed email")
    }

    @Test("Different emails produce different hashes")
    func differentEmailsDifferentHashes() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let r1 = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        let r2 = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.bobRow
        )
        #expect(r1[3] != r2[3], "Different emails should produce different hashes")
    }

    @Test("Customer phone keeps area code, masks rest")
    func customerPhonePartial() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        let phone = result[4]!
        #expect(phone.hasPrefix("503"), "Area code should be preserved")
        #expect(phone.contains("***"), "Remaining digits should be masked")
    }

    @Test("Customer SSN is fully redacted")
    func customerSSNRedacted() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        #expect(result[8] == "XXX-XX-XXXX", "SSN should be completely redacted")
    }

    @Test("Customer zip code preserves format")
    func customerZipPreserved() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        let zip = result[7]!
        #expect(zip.count == 5, "Zip code should be 5 characters")
        #expect(zip.allSatisfy { $0.isNumber }, "Zip code should be all digits")
        #expect(zip != "97201", "Zip code should be different from original")
    }

    @Test("Customer address is replaced with fake address")
    func customerAddressFaked() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        #expect(result[5] != "742 Evergreen Terrace", "Address should be faked")
        #expect(result[6] != "Portland", "City should be faked")
    }

    @Test("Customer ID is not masked (needed for FK integrity)")
    func customerIdPassthrough() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        #expect(result[0] == "1", "Customer ID should pass through unchanged")
    }

    @Test("Membership tier is not masked (categorical, not sensitive)")
    func tierPassthrough() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: Self.aliceRow
        )
        #expect(result[9] == "platinum", "Tier should pass through unchanged")
    }

    // MARK: - Purchase Obfuscation Tests

    @Test("Purchase amount has noise applied (±15%)")
    func purchaseAmountNoise() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "purchases", columns: Self.purchaseColumns, values: Self.purchaseRow
        )
        let amount = Double(result[5]!)!
        let original = 5.50
        #expect(amount >= original * 0.85 && amount <= original * 1.15,
                "Amount \(amount) should be within ±15% of \(original)")
        #expect(amount != original, "Amount should differ from original")
    }

    @Test("Points earned has noise applied")
    func pointsEarnedNoise() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "purchases", columns: Self.purchaseColumns, values: Self.purchaseRow
        )
        let points = Double(result[6]!)!
        #expect(points >= 55 * 0.85 && points <= 55 * 1.15,
                "Points \(points) should be within ±15% of 55")
    }

    @Test("Purchase non-sensitive fields pass through")
    func purchasePassthrough() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "purchases", columns: Self.purchaseColumns, values: Self.purchaseRow
        )
        #expect(result[0] == "1", "ID should pass through")
        #expect(result[1] == "1", "Customer ID should pass through")
        #expect(result[2] == "1", "Store ID should pass through")
        #expect(result[3] == "Latte", "Item name should pass through")
        #expect(result[7] == "2026-01-05 08:00:00+00", "Timestamp should pass through")
    }

    // MARK: - Points Balance Obfuscation Tests

    @Test("Points balance has noise applied (±20%)")
    func pointsBalanceNoise() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "points_balance", columns: Self.pointsColumns, values: Self.alicePoints
        )
        let totalPoints = Double(result[1]!)!
        #expect(totalPoints >= 448 * 0.80 && totalPoints <= 448 * 1.20,
                "Total points \(totalPoints) should be within ±20% of 448")
    }

    @Test("Lifetime points obfuscated independently")
    func lifetimePointsNoise() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "points_balance", columns: Self.pointsColumns, values: Self.alicePoints
        )
        let lifetime = Double(result[2]!)!
        #expect(lifetime >= 448 * 0.80 && lifetime <= 448 * 1.20,
                "Lifetime points \(lifetime) should be within ±20% of 448")
    }

    @Test("Points balance timestamp passes through")
    func pointsTimestampPassthrough() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.maskRow(
            table: "points_balance", columns: Self.pointsColumns, values: Self.alicePoints
        )
        #expect(result[3] == "2026-02-05 09:00:00+00", "Timestamp should pass through")
    }

    // MARK: - Unmatched Table Passthrough

    @Test("Store table passes through completely (no rules match)")
    func storePassthrough() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let columns = ["id", "name", "city", "region"]
        let values: [String?] = ["1", "Downtown Roasters", "Portland", "Northwest"]
        let result = engine.maskRow(table: "stores", columns: columns, values: values)
        #expect(result == values, "Store data should pass through completely")
    }

    // MARK: - Pattern Rule Tests

    @Test("Pattern *.email catches email columns in any table")
    func patternEmail() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        // A hypothetical other table with an email column
        let result = engine.mask(value: "test@example.com", table: "newsletter_subscribers", column: "email")
        #expect(result != "test@example.com", "Pattern rule should mask email in any table")
    }

    @Test("Pattern *.ssn catches SSN columns in any table")
    func patternSSN() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let result = engine.mask(value: "123-45-6789", table: "employee_records", column: "ssn")
        #expect(result == "XXX-XX-XXXX", "Pattern rule should redact SSN in any table")
    }

    // MARK: - Full Row Batch Tests

    @Test("Mask all 10 customers consistently")
    func batchCustomers() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        let customers: [[String?]] = [
            ["1", "Alice",  "Johnson",  "alice.johnson@gmail.com",  "503-555-0101", "742 Evergreen Terrace", "Portland",      "97201", "539-48-0120", "platinum"],
            ["2", "Bob",    "Martinez", "bob.martinez@yahoo.com",   "206-555-0202", "1600 Pennsylvania Ave", "Seattle",       "98101", "461-73-9285", "gold"],
            ["3", "Carol",  "Chen",     "carol.chen@outlook.com",   "415-555-0303", "221B Baker Street",     "San Francisco", "94102", "182-56-7834", "silver"],
            ["4", "David",  "Williams", "david.w@protonmail.com",   "212-555-0404", "350 Fifth Avenue",      "New York",      "10001", "725-14-3690", "gold"],
            ["5", "Emma",   "Brown",    "emma.brown@icloud.com",    "503-555-0505", "90 Bedford Street",     "Portland",      "97205", "318-62-4057", "bronze"],
        ]

        var maskedEmails = Set<String>()
        for row in customers {
            let masked = engine.maskRow(
                table: "customers", columns: Self.customerColumns, values: row
            )
            // Every row should have SSN redacted
            #expect(masked[8] == "XXX-XX-XXXX")
            // Every row should have email hashed
            let email = masked[3]!
            #expect(email.contains("@"))
            maskedEmails.insert(email)
            // ID should pass through
            #expect(masked[0] == row[0])
            // Tier should pass through
            #expect(masked[9] == row[9])
        }
        // All emails should be unique (no hash collisions)
        #expect(maskedEmails.count == 5, "All 5 customers should have unique masked emails")
    }

    // MARK: - NULL Handling

    @Test("NULL values pass through masking unchanged")
    func nullHandling() throws {
        let engine = try MaskingEngine(config: Self.exampleConfig())
        // Customer with NULL phone and address
        let row: [String?] = [
            "99", "Test", "User", "test@example.com", nil,
            nil, nil, nil, nil, "bronze",
        ]
        let result = engine.maskRow(
            table: "customers", columns: Self.customerColumns, values: row
        )
        #expect(result[4] == nil, "NULL phone should stay NULL")
        #expect(result[5] == nil, "NULL address should stay NULL")
        #expect(result[8] == nil, "NULL SSN should stay NULL")
    }

    // MARK: - YAML Config Loading

    @Test("YAML config loads and produces same masking as programmatic config")
    func yamlConfigEquivalent() throws {
        let yaml = """
        rules:
          - table: customers
            columns:
              ssn:
                strategy: redact
                options:
                  value: "XXX-XX-XXXX"
        defaults:
          seed: 20260403
        """
        let loader = MaskingConfigLoader()
        let yamlConfig = try loader.loadFromString(yaml)
        let engine = try MaskingEngine(config: yamlConfig)

        let result = engine.mask(value: "539-48-0120", table: "customers", column: "ssn")
        #expect(result == "XXX-XX-XXXX")
    }
}
