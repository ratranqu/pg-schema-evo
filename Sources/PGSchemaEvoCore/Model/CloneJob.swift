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
        force: Bool = false
    ) {
        self.source = source
        self.target = target
        self.objects = objects
        self.dryRun = dryRun
        self.defaultDataMethod = defaultDataMethod
        self.dataSizeThreshold = dataSizeThreshold
        self.dropIfExists = dropIfExists
        self.force = force
    }
}
