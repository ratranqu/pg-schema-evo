import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("DataSyncStateStore Tests")
struct DataSyncStateStoreTests {
    let store = DataSyncStateStore()

    @Test("Load valid state file")
    func loadValid() throws {
        let yaml = """
            tables:
              public.orders:
                column: updated_at
                last_value: "2026-03-30T12:00:00Z"
              public.events:
                column: id
                last_value: 984523
            """
        let path = try writeTempFile(yaml)
        let state = try store.load(path: path)

        #expect(state.tables.count == 2)
        #expect(state.tables["public.orders"]?.column == "updated_at")
        #expect(state.tables["public.orders"]?.lastValue == "2026-03-30T12:00:00Z")
        #expect(state.tables["public.events"]?.column == "id")
        #expect(state.tables["public.events"]?.lastValue == "984523")
    }

    @Test("Load file not found throws syncStateFileNotFound")
    func loadNotFound() {
        #expect(throws: PGSchemaEvoError.self) {
            try store.load(path: "/nonexistent/path/state.yaml")
        }
    }

    @Test("Load malformed YAML throws syncStateCorrupted")
    func loadMalformed() throws {
        let path = try writeTempFile("not: valid: yaml: [")
        #expect(throws: (any Error).self) {
            try store.load(path: path)
        }
    }

    @Test("Load missing tables section throws syncStateCorrupted")
    func loadMissingTables() throws {
        let path = try writeTempFile("version: 1\n")
        #expect(throws: PGSchemaEvoError.self) {
            try store.load(path: path)
        }
    }

    @Test("Save and load round-trip")
    func saveAndLoadRoundTrip() throws {
        let state = DataSyncState(tables: [
            "public.orders": DataSyncTableState(column: "updated_at", lastValue: "2026-03-30T12:00:00Z"),
            "public.events": DataSyncTableState(column: "id", lastValue: "984523"),
        ])

        let path = NSTemporaryDirectory() + "pg-schema-evo-test-\(UUID().uuidString).yaml"
        try store.save(state: state, path: path)
        let loaded = try store.load(path: path)

        #expect(loaded.tables.count == 2)
        #expect(loaded.tables["public.orders"]?.column == "updated_at")
        #expect(loaded.tables["public.orders"]?.lastValue == "2026-03-30T12:00:00Z")
        #expect(loaded.tables["public.events"]?.column == "id")
        #expect(loaded.tables["public.events"]?.lastValue == "984523")
    }

    @Test("Save empty state produces valid YAML")
    func saveEmptyState() throws {
        let state = DataSyncState(tables: [:])
        let path = NSTemporaryDirectory() + "pg-schema-evo-test-\(UUID().uuidString).yaml"
        try store.save(state: state, path: path)
        let loaded = try store.load(path: path)
        #expect(loaded.tables.isEmpty)
    }

    @Test("Load non-mapping root throws syncStateCorrupted")
    func loadNonMappingRoot() throws {
        let path = try writeTempFile("- item1\n- item2")
        #expect(throws: PGSchemaEvoError.self) {
            try store.load(path: path)
        }
    }

    @Test("Load invalid table entry throws syncStateCorrupted")
    func loadInvalidTableEntry() throws {
        let yaml = """
            tables:
              users: "just a string, not a mapping"
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try store.load(path: path)
        }
    }

    @Test("Load file that cannot be read throws syncStateCorrupted")
    func loadUnreadableFile() throws {
        let dirPath = NSTemporaryDirectory() + "pg-schema-evo-dir-\(UUID().uuidString).yaml"
        try FileManager.default.createDirectory(atPath: dirPath, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: dirPath) }
        #expect(throws: PGSchemaEvoError.self) {
            try store.load(path: dirPath)
        }
    }

    @Test("Integer last_value is preserved as string")
    func integerLastValue() throws {
        let yaml = """
            tables:
              public.counters:
                column: seq_id
                last_value: 42
            """
        let path = try writeTempFile(yaml)
        let state = try store.load(path: path)

        #expect(state.tables["public.counters"]?.lastValue == "42")
    }

    private func writeTempFile(_ content: String) throws -> String {
        let path = NSTemporaryDirectory() + "pg-schema-evo-test-\(UUID().uuidString).yaml"
        try content.write(toFile: path, atomically: true, encoding: .utf8)
        return path
    }
}
