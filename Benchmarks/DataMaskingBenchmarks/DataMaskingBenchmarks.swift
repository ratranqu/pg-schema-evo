import Benchmark
import DataMasking

// Pre-build the engine once for benchmarks.
private let benchmarkConfig: MaskingConfig = {
    var config = MaskingConfig()
    config.addTableRule(table: "users", columns: [
        "email": .strategy("hash"),
        "name": .strategy("fake"),
        "ssn": .strategy("redact", options: ["value": "XXX-XX-XXXX"]),
        "phone": .strategy("partial", options: ["type": "phone"]),
        "salary": .strategy("numeric-noise"),
    ])
    return config
}()

private let benchmarkEngine = try! MaskingEngine(config: benchmarkConfig)

// An engine with NO rules — used to verify zero-allocation passthrough.
private let emptyEngine = try! MaskingEngine(config: MaskingConfig())

// Sample data
private let sampleColumns = ["id", "email", "name", "ssn", "phone", "salary"]
private let sampleValues: [String?] = [
    "12345",
    "john.doe@example.com",
    "John Doe",
    "123-45-6789",
    "555-123-4567",
    "85000",
]

// Unmasked row (no columns match any rule)
private let unmatchedColumns = ["col_a", "col_b", "col_c", "col_d", "col_e", "col_f"]

let benchmarks: @Sendable () -> Void = {

    // MARK: - Throughput benchmarks

    Benchmark(
        "MaskRow - 6 columns with mixed strategies",
        configuration: .init(
            metrics: [
                .wallClock,
                .throughput,
                .mallocCountTotal,
            ]
        )
    ) { benchmark in
        for _ in benchmark.scaledIterations {
            let result = benchmarkEngine.maskRow(
                table: "users",
                columns: sampleColumns,
                values: sampleValues
            )
            blackHole(result)
        }
    }

    Benchmark(
        "MaskRow - passthrough (no rules match)",
        configuration: .init(
            metrics: [
                .wallClock,
                .throughput,
                .mallocCountTotal,
            ]
        )
    ) { benchmark in
        for _ in benchmark.scaledIterations {
            let result = emptyEngine.maskRow(
                table: "users",
                columns: sampleColumns,
                values: sampleValues
            )
            blackHole(result)
        }
    }

    Benchmark(
        "MaskRow - unmatched columns (zero-alloc expected)",
        configuration: .init(
            metrics: [
                .mallocCountTotal,
            ],
            thresholds: [
                .mallocCountTotal: .init(
                    absolute: [.p90: 0]
                ),
            ]
        )
    ) { benchmark in
        for _ in benchmark.scaledIterations {
            let result = benchmarkEngine.maskRow(
                table: "users",
                columns: unmatchedColumns,
                values: sampleValues
            )
            blackHole(result)
        }
    }

    // MARK: - Individual strategy benchmarks

    Benchmark(
        "HashStrategy - single value",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let strategy = HashStrategy()
        let ctx = MaskingContext(table: "users", column: "email", seed: 42)
        for _ in benchmark.scaledIterations {
            let result = strategy.mask("john.doe@example.com", context: ctx)
            blackHole(result)
        }
    }

    Benchmark(
        "RedactStrategy - single value",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let strategy = RedactStrategy()
        let ctx = MaskingContext(table: "t", column: "c")
        for _ in benchmark.scaledIterations {
            let result = strategy.mask("secret data", context: ctx)
            blackHole(result)
        }
    }

    Benchmark(
        "PartialStrategy - email",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let strategy = PartialStrategy(keep: 1, valueType: "email")
        let ctx = MaskingContext(table: "t", column: "c")
        for _ in benchmark.scaledIterations {
            let result = strategy.mask("john.doe@example.com", context: ctx)
            blackHole(result)
        }
    }

    Benchmark(
        "PreserveFormatStrategy - phone number",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let strategy = PreserveFormatStrategy()
        let ctx = MaskingContext(table: "t", column: "c", seed: 42)
        for _ in benchmark.scaledIterations {
            let result = strategy.mask("555-123-4567", context: ctx)
            blackHole(result)
        }
    }

    Benchmark(
        "FakeStrategy - name generation",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let strategy = FakeStrategy(dataType: "name")
        let ctx = MaskingContext(table: "users", column: "name", seed: 42)
        for _ in benchmark.scaledIterations {
            let result = strategy.mask("John Doe", context: ctx)
            blackHole(result)
        }
    }

    Benchmark(
        "NumericNoiseStrategy - integer",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let strategy = NumericNoiseStrategy()
        let ctx = MaskingContext(table: "t", column: "c", seed: 42)
        for _ in benchmark.scaledIterations {
            let result = strategy.mask("85000", context: ctx)
            blackHole(result)
        }
    }

    // MARK: - Value parser benchmarks

    Benchmark(
        "EmailParser - parse + print round-trip",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let parser = EmailParser()
        for _ in benchmark.scaledIterations {
            let parts = parser.parse("john.doe@example.com")!
            let result = parser.print(parts)
            blackHole(result)
        }
    }

    Benchmark(
        "PhoneParser - parse + print round-trip",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let parser = PhoneParser()
        for _ in benchmark.scaledIterations {
            let parts = parser.parse("555-123-4567")!
            let result = parser.print(parts)
            blackHole(result)
        }
    }

    // MARK: - DSL benchmarks

    Benchmark(
        "DSL parse + evaluate: hash(email.local) + @ + email.domain",
        configuration: .init(metrics: [.wallClock, .throughput, .mallocCountTotal])
    ) { benchmark in
        let dslParser = RuleExpressionParser()
        let evaluator = RuleExpressionEvaluator()
        let ctx = MaskingContext(table: "users", column: "email", seed: 42)
        for _ in benchmark.scaledIterations {
            let expr = try dslParser.parse("hash(email.local) + \"@\" + email.domain")
            let result = try evaluator.evaluate(expr, value: "john@example.com", context: ctx)
            blackHole(result)
        }
    }
}
