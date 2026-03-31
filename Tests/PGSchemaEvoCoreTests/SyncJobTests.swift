import Testing
@testable import PGSchemaEvoCore

@Suite("SyncJob Tests")
struct SyncJobTests {

    @Test("SyncJob defaults")
    func defaults() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: []
        )

        #expect(job.dryRun == true)
        #expect(job.dropExtra == false)
        #expect(job.dropIfExists == false)
        #expect(job.force == false)
        #expect(job.skipPreflight == false)
        #expect(job.syncAll == false)
        #expect(job.retries == 3)
    }

    @Test("SyncJob custom values")
    func customValues() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: [ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t"))],
            dryRun: false,
            dropExtra: true,
            dropIfExists: true,
            force: true,
            skipPreflight: true,
            syncAll: true,
            retries: 5
        )

        #expect(job.dryRun == false)
        #expect(job.dropExtra == true)
        #expect(job.dropIfExists == true)
        #expect(job.force == true)
        #expect(job.skipPreflight == true)
        #expect(job.syncAll == true)
        #expect(job.retries == 5)
        #expect(job.objects.count == 1)
    }

    @Test("toCloneJob converts correctly")
    func toCloneJob() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: [ObjectSpec(id: ObjectIdentifier(type: .table, schema: "public", name: "t"))],
            dryRun: false,
            dropIfExists: true,
            force: true,
            retries: 2
        )

        let cloneJob = job.toCloneJob()
        #expect(cloneJob.dryRun == false)
        #expect(cloneJob.dropIfExists == true)
        #expect(cloneJob.force == true)
        #expect(cloneJob.retries == 2)
        #expect(cloneJob.objects.count == 1)
    }
}
