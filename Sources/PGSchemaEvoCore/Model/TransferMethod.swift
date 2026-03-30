/// Method used to transfer table data between clusters.
public enum TransferMethod: String, Codable, Sendable {
    /// COPY via psql pipe (text-based, script-friendly).
    case copy
    /// pg_dump/pg_restore (binary, fast for large tables).
    case pgDump = "pgdump"
    /// Auto-select based on table size threshold.
    case auto
}
