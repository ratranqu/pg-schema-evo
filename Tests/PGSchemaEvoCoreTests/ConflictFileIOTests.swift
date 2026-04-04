import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ConflictFileIO Tests")
struct ConflictFileIOTests {

    private func makeReport() -> ConflictReport {
        let c1 = SchemaConflict(
            objectIdentifier: "table:public.users",
            kind: .extraInTarget,
            description: "Column legacy: extra in target",
            sourceSQL: ["ALTER TABLE \"public\".\"users\" DROP COLUMN \"legacy\";"],
            targetSQL: ["ALTER TABLE \"public\".\"users\" ADD COLUMN \"legacy\" text;"],
            isDestructive: true
        )
        let c2 = SchemaConflict(
            objectIdentifier: "table:public.users",
            kind: .divergedDefinition,
            description: "Column age: type integer -> bigint",
            sourceSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE bigint;"],
            targetSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE integer;"]
        )
        return ConflictReport(conflicts: [c1, c2])
    }

    @Test("Write and read round-trip preserves conflicts")
    func writeReadRoundTrip() throws {
        let report = makeReport()
        let tmpPath = NSTemporaryDirectory() + "conflict-test-\(UUID().uuidString).json"
        defer { try? FileManager.default.removeItem(atPath: tmpPath) }

        try ConflictFileIO.writeConflictFile(report: report, to: tmpPath)

        // File should exist and be valid JSON
        let data = try Data(contentsOf: URL(fileURLWithPath: tmpPath))
        #expect(!data.isEmpty)

        // Should have no resolutions yet
        let resolutions = try ConflictFileIO.readResolutions(from: tmpPath)
        #expect(resolutions.isEmpty)
    }

    @Test("Read resolutions from edited file")
    func readEditedResolutions() throws {
        let report = makeReport()
        let tmpPath = NSTemporaryDirectory() + "conflict-test-\(UUID().uuidString).json"
        defer { try? FileManager.default.removeItem(atPath: tmpPath) }

        try ConflictFileIO.writeConflictFile(report: report, to: tmpPath)

        // Edit the file to add resolutions
        var data = try Data(contentsOf: URL(fileURLWithPath: tmpPath))
        var json = String(data: data, encoding: .utf8)!
        // Replace null resolution with actual choices
        json = json.replacingOccurrences(
            of: "\"resolution\" : null",
            with: "\"resolution\" : \"apply-source\""
        )
        data = json.data(using: .utf8)!
        try data.write(to: URL(fileURLWithPath: tmpPath))

        let resolutions = try ConflictFileIO.readResolutions(from: tmpPath)
        #expect(resolutions.count == 2)
        #expect(resolutions.allSatisfy { $0.choice == .applySource })
    }

    @Test("Invalid file throws conflictFileParseError")
    func invalidFileThrows() throws {
        let tmpPath = NSTemporaryDirectory() + "conflict-test-\(UUID().uuidString).json"
        defer { try? FileManager.default.removeItem(atPath: tmpPath) }

        try "not valid json".data(using: .utf8)!.write(to: URL(fileURLWithPath: tmpPath))

        #expect(throws: PGSchemaEvoError.self) {
            try ConflictFileIO.readResolutions(from: tmpPath)
        }
    }

    @Test("Missing file throws conflictFileParseError")
    func missingFileThrows() {
        #expect(throws: PGSchemaEvoError.self) {
            try ConflictFileIO.readResolutions(from: "/nonexistent/path.json")
        }
    }

    @Test("matchResolutions matches by object+kind")
    func matchResolutions() {
        let report = makeReport()
        let c2Id = report.conflicts[1].id

        // Simulate file conflicts with different UUIDs but same object+kind
        let fileC1Id = UUID()
        let staleId = UUID()
        let fileConflicts = [
            SchemaConflict(
                id: fileC1Id,
                objectIdentifier: "table:public.users",
                kind: .extraInTarget,
                description: "Column legacy: extra in target",
                sourceSQL: ["DROP ..."],
                isDestructive: true
            ),
            SchemaConflict(
                id: staleId,
                objectIdentifier: "table:public.stale",
                kind: .extraInTarget,
                description: "Stale conflict",
                sourceSQL: ["DROP ..."],
                isDestructive: true
            )
        ]
        let fileResolutions = [
            ConflictResolution(conflictId: fileC1Id, choice: .applySource),
            ConflictResolution(conflictId: staleId, choice: .keepTarget) // stale, should be ignored
        ]

        let (matched, unresolved) = ConflictFileIO.matchResolutions(
            fileResolutions: fileResolutions,
            fileConflicts: fileConflicts,
            report: report
        )

        #expect(matched.count == 1)
        #expect(matched[0].conflictId == report.conflicts[0].id) // Mapped to current ID
        #expect(matched[0].choice == .applySource)
        #expect(unresolved.count == 1)
        #expect(unresolved[0].id == c2Id)
    }

    @Test("readConflicts from missing file throws")
    func readConflictsMissingFile() {
        #expect(throws: PGSchemaEvoError.self) {
            try ConflictFileIO.readConflicts(from: "/nonexistent/conflicts.json")
        }
    }

    @Test("readConflicts from invalid JSON throws")
    func readConflictsInvalidJSON() throws {
        let tmpPath = NSTemporaryDirectory() + "conflict-test-\(UUID().uuidString).json"
        defer { try? FileManager.default.removeItem(atPath: tmpPath) }

        try "not valid json".data(using: .utf8)!.write(to: URL(fileURLWithPath: tmpPath))

        #expect(throws: PGSchemaEvoError.self) {
            try ConflictFileIO.readConflicts(from: tmpPath)
        }
    }
}
