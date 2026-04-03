import Foundation

/// Adds random noise to numeric values to obfuscate aggregates.
///
/// The noise is controlled by a percentage range (default ±10%).
/// Uses a deterministic PRNG seeded from the context for reproducibility.
public struct NumericNoiseStrategy: MaskingStrategy, Sendable {
    public static let name = "numeric-noise"

    /// Noise as a fraction (0.1 = ±10%). Defaults to 0.1.
    public let noiseFraction: Double
    /// If true, use deterministic noise based on context seed + value.
    public let deterministic: Bool

    public init(noiseFraction: Double = 0.1, deterministic: Bool = true) {
        self.noiseFraction = noiseFraction
        self.deterministic = deterministic
    }

    public init(options: [String: String]) {
        self.noiseFraction = options["noise"]
            .flatMap(Double.init) ?? 0.1
        self.deterministic = options["deterministic"]
            .map { $0.lowercased() != "false" } ?? true
    }

    public func mask(_ value: String, context: MaskingContext) -> String? {
        // Try parsing as Double
        guard let number = Double(value) else {
            return value // non-numeric passthrough
        }

        let noise: Double
        if deterministic {
            // Deterministic noise from FNV-1a of (value + seed)
            let hash = fnv1a64(value + String(context.seed))
            // Map hash to [-1, 1] range
            let normalized = Double(Int64(bitPattern: hash)) / Double(Int64.max)
            noise = normalized * noiseFraction
        } else {
            noise = Double.random(in: -noiseFraction...noiseFraction)
        }

        let result = number * (1.0 + noise)

        // Preserve integer formatting if input was an integer
        if !value.contains(".") && !value.contains("e") && !value.contains("E") {
            return String(Int64(result.rounded()))
        }

        // Preserve decimal precision
        let decimalPlaces = value.split(separator: ".").last.map(\.count) ?? 0
        return String(format: "%.\(decimalPlaces)f", result)
    }
}
