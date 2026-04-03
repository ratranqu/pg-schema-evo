import Foundation
import Logging
import DataMasking

/// Intercepts COPY-format data (tab-delimited) and applies masking per row.
///
/// Integrates the `DataMasking` library into `pg-schema-evo`'s data pipeline.
public struct MaskingDataTransfer: Sendable {
    private let engine: MaskingEngine
    private let logger: Logger

    public init(engine: MaskingEngine, logger: Logger) {
        self.engine = engine
        self.logger = logger
    }

    /// Mask rows in PostgreSQL COPY format (tab-delimited, `\N` for NULL).
    /// - Parameters:
    ///   - data: The raw COPY output (tab-delimited rows, newline-separated).
    ///   - tableName: The table these rows belong to.
    ///   - columnNames: Ordered column names matching the COPY output.
    /// - Returns: Masked COPY output in the same format.
    public func maskCopyData(
        data: String,
        tableName: String,
        columnNames: [String]
    ) -> String {
        let lines = data.split(separator: "\n", omittingEmptySubsequences: false)
        var result: [String] = []
        result.reserveCapacity(lines.count)

        for line in lines {
            if line.isEmpty || line == "\\." {
                result.append(String(line))
                continue
            }

            let fields = line.split(separator: "\t", omittingEmptySubsequences: false)
            let values: [String?] = fields.map { field in
                field == "\\N" ? nil : String(field)
            }

            let masked = engine.maskRow(
                table: tableName,
                columns: columnNames,
                values: values
            )

            let maskedLine = masked.map { value in
                value ?? "\\N"
            }.joined(separator: "\t")

            result.append(maskedLine)
        }

        return result.joined(separator: "\n")
    }

    /// Load a MaskingEngine from a YAML configuration file path.
    /// - Parameters:
    ///   - configPath: Path to the masking YAML config.
    ///   - registry: Optional custom strategy registry.
    /// - Returns: A configured MaskingEngine.
    public static func loadEngine(
        configPath: String,
        registry: StrategyRegistry = StrategyRegistry()
    ) throws -> MaskingEngine {
        let loader = MaskingConfigLoader()
        let config = try loader.load(from: configPath)
        return try MaskingEngine(config: config, registry: registry)
    }
}
