/// The specification of an incremental sync operation.
public struct SyncJob: Sendable {
    public let source: ConnectionConfig
    public let target: ConnectionConfig
    /// Objects to sync. When `syncAll` is true, these are used as type/schema filters.
    public let objects: [ObjectSpec]
    public let dryRun: Bool
    /// Drop objects on the target that don't exist on the source.
    public let dropExtra: Bool
    /// Drop objects before creating them (for objects only in source).
    public let dropIfExists: Bool
    /// Allow dropping columns, constraints, indexes, triggers, and policies that exist
    /// in the target but not in the source. Without this flag, destructive per-object
    /// changes are reported but not applied.
    public let allowDropColumns: Bool
    /// Skip interactive confirmation prompt.
    public let force: Bool
    /// Skip pre-flight validation checks.
    public let skipPreflight: Bool
    /// Sync all objects matching the type/schema filters (not just listed objects).
    public let syncAll: Bool
    /// Maximum retry attempts for transient errors.
    public let retries: Int

    public init(
        source: ConnectionConfig,
        target: ConnectionConfig,
        objects: [ObjectSpec],
        dryRun: Bool = true,
        dropExtra: Bool = false,
        dropIfExists: Bool = false,
        allowDropColumns: Bool = false,
        force: Bool = false,
        skipPreflight: Bool = false,
        syncAll: Bool = false,
        retries: Int = 3
    ) {
        self.source = source
        self.target = target
        self.objects = objects
        self.dryRun = dryRun
        self.dropExtra = dropExtra
        self.dropIfExists = dropIfExists
        self.allowDropColumns = allowDropColumns
        self.force = force
        self.skipPreflight = skipPreflight
        self.syncAll = syncAll
        self.retries = retries
    }

    /// Convert to a CloneJob for compatibility with ScriptRenderer and LiveExecutor.
    public func toCloneJob() -> CloneJob {
        CloneJob(
            source: source,
            target: target,
            objects: objects,
            dryRun: dryRun,
            dropIfExists: dropIfExists,
            force: force,
            retries: retries,
            skipPreflight: skipPreflight
        )
    }
}
