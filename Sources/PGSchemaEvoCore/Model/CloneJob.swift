/// The full specification of a clone operation, built from CLI args or a config file.
public struct CloneJob: Sendable {
    public let source: ConnectionConfig
    public let target: ConnectionConfig
    public let objects: [ObjectSpec]
    public let dryRun: Bool
    public let defaultDataMethod: TransferMethod
    /// Size threshold in bytes for auto method selection.
    public let dataSizeThreshold: Int
    public let dropIfExists: Bool
    /// Skip interactive confirmation prompt.
    public let force: Bool
    /// Maximum retry attempts for transient errors (0 = no retries).
    public let retries: Int
    /// Skip pre-flight validation checks.
    public let skipPreflight: Bool
    /// Global row limit for data copy (per-object overrides take precedence).
    public let globalRowLimit: Int?
    /// Maximum parallel data transfers. 0 = auto-detect, 1 = sequential.
    public let parallel: Int
    /// Strategy for resolving schema conflicts (default: .fail).
    public let conflictStrategy: ConflictStrategy
    /// Auto-accept non-destructive resolutions in interactive mode (--yes).
    public let autoAcceptNonDestructive: Bool
    /// Path to write conflict report for offline review (--conflict-file).
    public let conflictFilePath: String?
    /// Path to read resolutions from a previously generated conflict file (--resolve-from).
    public let resolveFromPath: String?
    /// Whether conflict resolution was explicitly requested via CLI flags.
    public let conflictResolutionExplicit: Bool

    /// Default threshold: 100 MB.
    public static let defaultDataSizeThreshold = 100 * 1024 * 1024

    public init(
        source: ConnectionConfig,
        target: ConnectionConfig,
        objects: [ObjectSpec],
        dryRun: Bool = true,
        defaultDataMethod: TransferMethod = .auto,
        dataSizeThreshold: Int = CloneJob.defaultDataSizeThreshold,
        dropIfExists: Bool = false,
        force: Bool = false,
        retries: Int = 3,
        skipPreflight: Bool = false,
        globalRowLimit: Int? = nil,
        parallel: Int = 0,
        conflictStrategy: ConflictStrategy = .fail,
        autoAcceptNonDestructive: Bool = false,
        conflictFilePath: String? = nil,
        resolveFromPath: String? = nil,
        conflictResolutionExplicit: Bool = false
    ) {
        self.source = source
        self.target = target
        self.objects = objects
        self.dryRun = dryRun
        self.defaultDataMethod = defaultDataMethod
        self.dataSizeThreshold = dataSizeThreshold
        self.dropIfExists = dropIfExists
        self.force = force
        self.retries = retries
        self.skipPreflight = skipPreflight
        self.globalRowLimit = globalRowLimit
        self.parallel = parallel
        self.conflictStrategy = conflictStrategy
        self.autoAcceptNonDestructive = autoAcceptNonDestructive
        self.conflictFilePath = conflictFilePath
        self.resolveFromPath = resolveFromPath
        self.conflictResolutionExplicit = conflictResolutionExplicit
    }
}
