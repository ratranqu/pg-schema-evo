# Data Masking Example: Coffee Shop Loyalty Program

This example demonstrates how to clone a production database while
anonymizing personally identifiable information (PII) and obfuscating
sensitive business metrics like purchase counts and point balances.

## The Problem

A coffee shop loyalty program database contains:

| Data | Sensitivity | Risk |
|------|-------------|------|
| Customer names, emails, phones | **PII** | Identity theft, privacy violation |
| Social Security Numbers | **Highly sensitive PII** | Identity fraud |
| Addresses, zip codes | **PII** | Location tracking |
| Purchase counts per customer | **Behavioral** | Identifies individuals by habit patterns |
| Point balances | **Behavioral** | Heavy buyers identifiable by high totals |

You want to share this database with your analytics team or use it in
development, but you can't expose the raw PII or let someone reverse-engineer
which anonymized customer is "the person who buys 8 coffees per month."

## The Solution

The `DataMasking` library applies per-column strategies during data transfer:

| Column | Strategy | Effect |
|--------|----------|--------|
| `first_name`, `last_name` | `fake` | Realistic but fictional names |
| `email` | DSL expression | Hash local part, keep domain: `alice.j@gmail.com` → `a3f9e2c@gmail.com` |
| `phone` | `partial` | Keep area code: `503-555-0101` → `503-***-****` |
| `address`, `city` | `fake` | Fictional addresses |
| `zip_code` | `preserve-format` | Random digits, same format: `97201` → `38475` |
| `ssn` | `redact` | Replaced with `XXX-XX-XXXX` |
| `amount`, `points_earned` | `numeric-noise` | ±15% random variation |
| `total_points`, `lifetime_points` | `numeric-noise` | ±20% random variation |
| `item_count` | `numeric-noise` | ±10% random variation |

Non-sensitive data passes through unchanged: store names, item names,
timestamps, membership tiers, and all schema structure.

## Setup

### 1. Start the databases

```bash
# Start source and target PostgreSQL instances
docker compose -f docker/docker-compose.yml up -d
```

### 2. Seed the source database

```bash
# Load the coffee shop schema and sample data
psql "postgresql://testuser:testpass@localhost:5432/source_db" \
  -f examples/data-masking/seed-source.sql
```

### 3. Verify source data (contains real PII)

```bash
psql "postgresql://testuser:testpass@localhost:5432/source_db" -c \
  "SELECT first_name, last_name, email, phone, ssn, tier
   FROM loyalty.customers LIMIT 5;"
```

Expected output:
```
 first_name | last_name |          email           |    phone     |     ssn      |   tier
------------+-----------+--------------------------+--------------+--------------+----------
 Alice      | Johnson   | alice.johnson@gmail.com  | 503-555-0101 | 539-48-0120  | platinum
 Bob        | Martinez  | bob.martinez@yahoo.com   | 206-555-0202 | 461-73-9285  | gold
 Carol      | Chen      | carol.chen@outlook.com   | 415-555-0303 | 182-56-7834  | silver
 David      | Williams  | david.w@protonmail.com   | 212-555-0404 | 725-14-3690  | gold
 Emma       | Brown     | emma.brown@icloud.com    | 503-555-0505 | 318-62-4057  | bronze
```

### 4. Clone with masking

```bash
pg-schema-evo mask \
  --source-dsn "postgresql://testuser:testpass@localhost:5432/source_db" \
  --target-dsn "postgresql://testuser:testpass@localhost:5433/target_db" \
  --masking-config examples/data-masking/masking-config.yaml \
  --table loyalty.customers \
  --table loyalty.purchases \
  --table loyalty.points_balance \
  --table loyalty.redemptions
```

### 5. Verify masked data (PII removed)

```bash
psql "postgresql://testuser:testpass@localhost:5433/target_db" -c \
  "SELECT first_name, last_name, email, phone, ssn, tier
   FROM loyalty.customers LIMIT 5;"
```

Expected output (values will vary due to deterministic hashing):
```
 first_name | last_name |           email            |    phone     |     ssn      |   tier
------------+-----------+----------------------------+--------------+--------------+----------
 Grace      | Garcia    | 4a8f2e1c903b@gmail.com     | 503-***-**** | XXX-XX-XXXX  | platinum
 Noah       | Thomas    | 7c3d91e5a2f0@yahoo.com     | 206-***-**** | XXX-XX-XXXX  | gold
 Mia        | Wilson    | b2e8f5047d1a@outlook.com   | 415-***-**** | XXX-XX-XXXX  | silver
 Sam        | Martinez  | e61a4cf82b9d@protonmail.com| 212-***-**** | XXX-XX-XXXX  | gold
 Vera       | Anderson  | 8d5b3e72c1f0@icloud.com   | 503-***-**** | XXX-XX-XXXX  | bronze
```

Key observations:
- **Names** are completely different (fake strategy)
- **Emails** have hashed local parts but original domains preserved
- **Phones** show area codes but rest is masked
- **SSNs** are fully redacted
- **Tier** is unchanged (not sensitive)

### 6. Verify purchase obfuscation

```bash
# Source: exact amounts
psql "postgresql://testuser:testpass@localhost:5432/source_db" -c \
  "SELECT customer_id, count(*) as purchases, sum(amount) as total
   FROM loyalty.purchases GROUP BY customer_id ORDER BY customer_id;"

# Target: amounts shifted by ±15%
psql "postgresql://testuser:testpass@localhost:5433/target_db" -c \
  "SELECT customer_id, count(*) as purchases, sum(amount) as total
   FROM loyalty.purchases GROUP BY customer_id ORDER BY customer_id;"
```

The purchase counts stay the same (row count isn't masked), but the
dollar amounts and point values will be different — close enough for
realistic analytics, but not exact enough to match back to individuals.

## Programmatic Usage (Swift)

```swift
import DataMasking

// Build config programmatically (no YAML file needed)
var config = MaskingConfig()
config.addTableRule(table: "customers", columns: [
    "first_name": .strategy("fake", options: ["type": "first_name"]),
    "last_name":  .strategy("fake", options: ["type": "last_name"]),
    "email":      .expression("hash(email.local) + \"@\" + email.domain"),
    "phone":      .strategy("partial", options: ["type": "phone"]),
    "ssn":        .strategy("redact", options: ["value": "XXX-XX-XXXX"]),
    "address":    .strategy("fake", options: ["type": "address"]),
    "zip_code":   .strategy("preserve-format"),
])
config.addTableRule(table: "purchases", columns: [
    "amount":        .strategy("numeric-noise", options: ["noise": "0.15"]),
    "points_earned": .strategy("numeric-noise", options: ["noise": "0.15"]),
])
config.addTableRule(table: "points_balance", columns: [
    "total_points":    .strategy("numeric-noise", options: ["noise": "0.20"]),
    "lifetime_points": .strategy("numeric-noise", options: ["noise": "0.20"]),
])

let engine = try MaskingEngine(config: config)

// Mask a single row
let masked = engine.maskRow(
    table: "customers",
    columns: ["id", "first_name", "last_name", "email", "phone", "ssn", "tier"],
    values:  ["1",  "Alice",      "Johnson",   "alice.johnson@gmail.com", "503-555-0101", "539-48-0120", "platinum"]
)
// Result: ["1", "Grace", "Garcia", "4a8f2e1c903b@gmail.com", "503-***-****", "XXX-XX-XXXX", "platinum"]
//          ^id   ^fake    ^fake     ^hashed local@real domain  ^partial        ^redacted       ^passthrough
```

## Custom Strategy Example

Register your own masking strategy:

```swift
import DataMasking

struct TruncateStrategy: MaskingStrategy {
    static let name = "truncate"
    let maxLength: Int

    init(options: [String: String] = [:]) {
        self.maxLength = options["max"].flatMap(Int.init) ?? 10
    }

    func mask(_ value: String, context: MaskingContext) -> String? {
        String(value.prefix(maxLength))
    }
}

var registry = StrategyRegistry()
registry.register(name: "truncate") { opts in TruncateStrategy(options: opts) }

var config = MaskingConfig()
config.addTableRule(table: "logs", columns: [
    "message": .strategy("truncate", options: ["max": "50"]),
])

let engine = try MaskingEngine(config: config, registry: registry)
```

## What's Preserved vs. Masked

| Category | Examples | Masked? |
|----------|----------|---------|
| Schema structure | Tables, indexes, views, constraints | No (cloned as-is) |
| Primary keys | `id` columns | No (needed for FK integrity) |
| Foreign keys | `customer_id`, `store_id` | No (referential integrity) |
| Timestamps | `joined_at`, `purchased_at` | No (needed for time-series analysis) |
| Enums/labels | `tier`, `item_name`, store `name` | No (categorical data) |
| Names | `first_name`, `last_name` | Yes — fake replacement |
| Contact info | `email`, `phone` | Yes — hash/partial |
| Government IDs | `ssn` | Yes — full redaction |
| Addresses | `address`, `city`, `zip_code` | Yes — fake/format-preserving |
| Dollar amounts | `amount`, `total` | Yes — ±15% noise |
| Point counts | `total_points`, `lifetime_points` | Yes — ±20% noise |
