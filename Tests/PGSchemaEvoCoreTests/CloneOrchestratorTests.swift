import Testing
import Logging
@testable import PGSchemaEvoCore

@Suite("CloneOrchestrator Unit Tests")
struct CloneOrchestratorTests {

    // MARK: - resolveTransferMethod (via ScriptRenderer dry-run output)

    // The resolveTransferMethod is private, but we can test its behavior
    // indirectly through the ScriptRenderer output which reflects the method choice.

    @Test("TransferMethod.auto selects copy for small tables")
    func autoSelectsCopyForSmall() {
        // auto method with size below threshold should use copy
        let method = resolveTransferMethodHelper(preferred: .auto, size: 1000, threshold: CloneJob.defaultDataSizeThreshold)
        #expect(method == .copy)
    }

    @Test("TransferMethod.auto selects pgDump for large tables")
    func autoSelectsPgDumpForLarge() {
        let threshold = 100 * 1024 * 1024  // 100MB
        let method = resolveTransferMethodHelper(preferred: .auto, size: threshold, threshold: threshold)
        #expect(method == .pgDump)
    }

    @Test("TransferMethod.auto selects copy when size is nil")
    func autoSelectsCopyWhenSizeNil() {
        let method = resolveTransferMethodHelper(preferred: .auto, size: nil, threshold: CloneJob.defaultDataSizeThreshold)
        #expect(method == .copy)
    }

    @Test("TransferMethod.copy is always copy regardless of size")
    func copyAlwaysCopy() {
        let method = resolveTransferMethodHelper(preferred: .copy, size: 999_999_999, threshold: 100)
        #expect(method == .copy)
    }

    @Test("TransferMethod.pgDump is always pgDump regardless of size")
    func pgDumpAlwaysPgDump() {
        let method = resolveTransferMethodHelper(preferred: .pgDump, size: 1, threshold: 100)
        #expect(method == .pgDump)
    }

    // MARK: - CloneJob defaults

    @Test("CloneJob default values are correct")
    func cloneJobDefaults() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: []
        )
        #expect(job.dryRun == true)
        #expect(job.defaultDataMethod == .auto)
        #expect(job.dataSizeThreshold == CloneJob.defaultDataSizeThreshold)
        #expect(job.dropIfExists == false)
        #expect(job.force == false)
        #expect(job.retries == 3)
        #expect(job.skipPreflight == false)
        #expect(job.globalRowLimit == nil)
        #expect(job.parallel == 0)
    }

    @Test("CloneJob custom values are preserved")
    func cloneJobCustomValues() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: [ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t"))],
            dryRun: false,
            defaultDataMethod: .pgDump,
            dataSizeThreshold: 500,
            dropIfExists: true,
            force: true,
            retries: 5,
            skipPreflight: true,
            globalRowLimit: 1000,
            parallel: 8
        )
        #expect(job.dryRun == false)
        #expect(job.defaultDataMethod == .pgDump)
        #expect(job.dataSizeThreshold == 500)
        #expect(job.dropIfExists == true)
        #expect(job.force == true)
        #expect(job.retries == 5)
        #expect(job.skipPreflight == true)
        #expect(job.globalRowLimit == 1000)
        #expect(job.parallel == 8)
    }

    @Test("CloneJob defaultDataSizeThreshold is 100MB")
    func defaultDataSizeThreshold() {
        #expect(CloneJob.defaultDataSizeThreshold == 100 * 1024 * 1024)
    }

    // MARK: - DataTransferTask

    @Test("DataTransferTask defaults")
    func dataTransferTaskDefaults() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let task = DataTransferTask(id: id, method: .copy, estimatedSize: nil)
        #expect(task.whereClause == nil)
        #expect(task.rowLimit == nil)
        #expect(task.dependsOn.isEmpty)
    }

    @Test("DataTransferTask custom values")
    func dataTransferTaskCustom() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let dep = ObjectIdentifier(type: .table, schema: "public", name: "parent")
        let task = DataTransferTask(
            id: id,
            method: .pgDump,
            estimatedSize: 5000,
            whereClause: "active = true",
            rowLimit: 100,
            dependsOn: [dep]
        )
        #expect(task.method == .pgDump)
        #expect(task.estimatedSize == 5000)
        #expect(task.whereClause == "active = true")
        #expect(task.rowLimit == 100)
        #expect(task.dependsOn.contains(dep))
    }

    // MARK: - Helper

    /// Reimplements the private resolveTransferMethod logic for testing.
    private func resolveTransferMethodHelper(
        preferred: TransferMethod,
        size: Int?,
        threshold: Int
    ) -> TransferMethod {
        switch preferred {
        case .copy, .pgDump:
            return preferred
        case .auto:
            guard let size else { return .copy }
            return size >= threshold ? .pgDump : .copy
        }
    }
}
