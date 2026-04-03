/// Contextual information provided to a masking strategy for each value.
public struct MaskingContext: Sendable {
    /// The table name.
    public let table: String
    /// The column name.
    public let column: String
    /// Strategy-specific options from configuration.
    public let options: [String: String]
    /// A deterministic seed for hash-based strategies (derived from table+column).
    public let seed: UInt64

    public init(table: String, column: String, options: [String: String] = [:], seed: UInt64 = 0) {
        self.table = table
        self.column = column
        self.options = options
        self.seed = seed
    }

    /// Qualified reference: "table.column".
    public var qualifiedColumn: String { "\(table).\(column)" }
}
