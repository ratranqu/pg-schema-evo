/// The specification of an incremental data sync operation.
public struct DataSyncJob: Sendable {
    public let source: ConnectionConfig
    public let target: ConnectionConfig
    /// Tables to sync, each with its tracking column.
    public let tables: [DataSyncTableConfig]
    /// Path to the sync state file.
    public let stateFilePath: String
    public let dryRun: Bool
    /// Whether to detect and delete rows on the target that no longer exist on the source.
    public let detectDeletes: Bool
    /// Skip interactive confirmation prompt.
    public let force: Bool
    /// Maximum retry attempts for transient errors.
    public let retries: Int

    public init(
        source: ConnectionConfig,
        target: ConnectionConfig,
        tables: [DataSyncTableConfig],
        stateFilePath: String = ".pg-schema-evo-sync-state.yaml",
        dryRun: Bool = false,
        detectDeletes: Bool = false,
        force: Bool = false,
        retries: Int = 3
    ) {
        self.source = source
        self.target = target
        self.tables = tables
        self.stateFilePath = stateFilePath
        self.dryRun = dryRun
        self.detectDeletes = detectDeletes
        self.force = force
        self.retries = retries
    }
}

/// Per-table configuration for incremental data sync.
public struct DataSyncTableConfig: Sendable {
    public let id: ObjectIdentifier
    /// The column used for change detection (e.g. "updated_at" or "id").
    public let trackingColumn: String

    public init(id: ObjectIdentifier, trackingColumn: String) {
        self.id = id
        self.trackingColumn = trackingColumn
    }
}

/// Persisted state for the data sync — tracks the last synced value per table.
public struct DataSyncState: Sendable, Codable {
    /// Key is "schema.name", value is the per-table state.
    public var tables: [String: DataSyncTableState]

    public init(tables: [String: DataSyncTableState] = [:]) {
        self.tables = tables
    }
}

/// Per-table sync state: which column is tracked and the last synced value.
public struct DataSyncTableState: Sendable, Codable {
    public let column: String
    public let lastValue: String

    public init(column: String, lastValue: String) {
        self.column = column
        self.lastValue = lastValue
    }
}
