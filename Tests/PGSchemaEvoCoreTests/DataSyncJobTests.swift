import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("DataSyncJob Tests")
struct DataSyncJobTests {

    @Test("DataSyncJob defaults")
    func defaults() throws {
        let job = DataSyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            tables: []
        )

        #expect(job.stateFilePath == ".pg-schema-evo-sync-state.yaml")
        #expect(job.dryRun == false)
        #expect(job.detectDeletes == false)
        #expect(job.force == false)
        #expect(job.retries == 3)
    }

    @Test("DataSyncJob custom values")
    func customValues() throws {
        let table = DataSyncTableConfig(
            id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            trackingColumn: "updated_at"
        )

        let job = DataSyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            tables: [table],
            stateFilePath: "custom-state.yaml",
            dryRun: true,
            detectDeletes: true,
            force: true,
            retries: 5
        )

        #expect(job.stateFilePath == "custom-state.yaml")
        #expect(job.dryRun == true)
        #expect(job.detectDeletes == true)
        #expect(job.force == true)
        #expect(job.retries == 5)
        #expect(job.tables.count == 1)
        #expect(job.tables[0].trackingColumn == "updated_at")
    }

    @Test("DataSyncTableConfig stores correct values")
    func tableConfig() {
        let id = ObjectIdentifier(type: .table, schema: "analytics", name: "events")
        let config = DataSyncTableConfig(id: id, trackingColumn: "event_id")

        #expect(config.id.schema == "analytics")
        #expect(config.id.name == "events")
        #expect(config.trackingColumn == "event_id")
    }

    @Test("DataSyncState round-trips through Codable")
    func stateRoundTrip() throws {
        let state = DataSyncState(tables: [
            "public.orders": DataSyncTableState(column: "updated_at", lastValue: "2026-03-30T12:00:00Z"),
            "public.events": DataSyncTableState(column: "id", lastValue: "984523"),
        ])

        let encoder = JSONEncoder()
        let data = try encoder.encode(state)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(DataSyncState.self, from: data)

        #expect(decoded.tables.count == 2)
        #expect(decoded.tables["public.orders"]?.column == "updated_at")
        #expect(decoded.tables["public.orders"]?.lastValue == "2026-03-30T12:00:00Z")
        #expect(decoded.tables["public.events"]?.column == "id")
        #expect(decoded.tables["public.events"]?.lastValue == "984523")
    }
}
