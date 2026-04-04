import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("Conflict Model Tests")
struct ConflictModelTests {

    // MARK: - ConflictKind

    @Test("ConflictKind raw values")
    func conflictKindRawValues() {
        #expect(ConflictKind.extraInTarget.rawValue == "extraInTarget")
        #expect(ConflictKind.divergedDefinition.rawValue == "divergedDefinition")
        #expect(ConflictKind.objectOnlyInTarget.rawValue == "objectOnlyInTarget")
        #expect(ConflictKind.destructiveChange.rawValue == "destructiveChange")
        #expect(ConflictKind.irreversibleChange.rawValue == "irreversibleChange")
    }

    @Test("ConflictKind allCases")
    func conflictKindAllCases() {
        #expect(ConflictKind.allCases.count == 5)
    }

    // MARK: - ConflictStrategy

    @Test("ConflictStrategy raw values")
    func conflictStrategyRawValues() {
        #expect(ConflictStrategy.fail.rawValue == "fail")
        #expect(ConflictStrategy.sourceWins.rawValue == "source-wins")
        #expect(ConflictStrategy.targetWins.rawValue == "target-wins")
        #expect(ConflictStrategy.interactive.rawValue == "interactive")
        #expect(ConflictStrategy.skip.rawValue == "skip")
    }

    @Test("ConflictStrategy allCases")
    func conflictStrategyAllCases() {
        #expect(ConflictStrategy.allCases.count == 5)
    }

    @Test("ConflictStrategy init from rawValue")
    func conflictStrategyFromRaw() {
        #expect(ConflictStrategy(rawValue: "source-wins") == .sourceWins)
        #expect(ConflictStrategy(rawValue: "target-wins") == .targetWins)
        #expect(ConflictStrategy(rawValue: "invalid") == nil)
    }

    // MARK: - ResolutionChoice

    @Test("ResolutionChoice raw values")
    func resolutionChoiceRawValues() {
        #expect(ResolutionChoice.applySource.rawValue == "apply-source")
        #expect(ResolutionChoice.keepTarget.rawValue == "keep-target")
        #expect(ResolutionChoice.skip.rawValue == "skip")
    }

    // MARK: - SchemaConflict

    @Test("SchemaConflict defaults")
    func schemaConflictDefaults() {
        let conflict = SchemaConflict(
            objectIdentifier: "table:public.users",
            kind: .divergedDefinition,
            description: "Test",
            sourceSQL: ["SQL"]
        )
        #expect(!conflict.isDestructive)
        #expect(!conflict.isIrreversible)
        #expect(conflict.detail == nil)
        #expect(conflict.targetSQL.isEmpty)
    }

    @Test("SchemaConflict equality")
    func schemaConflictEquality() {
        let id = UUID()
        let c1 = SchemaConflict(id: id, objectIdentifier: "table:public.t", kind: .extraInTarget, description: "d", sourceSQL: ["s"])
        let c2 = SchemaConflict(id: id, objectIdentifier: "table:public.t", kind: .extraInTarget, description: "d", sourceSQL: ["s"])
        #expect(c1 == c2)
    }

    @Test("SchemaConflict Codable round-trip")
    func schemaConflictCodable() throws {
        let conflict = SchemaConflict(
            objectIdentifier: "table:public.users",
            kind: .destructiveChange,
            description: "Column dropped",
            sourceSQL: ["DROP COLUMN ..."],
            targetSQL: ["ADD COLUMN ..."],
            isDestructive: true,
            detail: "column: legacy"
        )
        let encoder = JSONEncoder()
        let data = try encoder.encode(conflict)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(SchemaConflict.self, from: data)
        #expect(decoded == conflict)
    }

    // MARK: - ConflictResolution

    @Test("ConflictResolution Codable round-trip")
    func conflictResolutionCodable() throws {
        let id = UUID()
        let resolution = ConflictResolution(
            conflictId: id,
            choice: .applySource,
            timestamp: Date()
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(resolution)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(ConflictResolution.self, from: data)
        #expect(decoded.conflictId == id)
        #expect(decoded.choice == .applySource)
        // Date comparison with tolerance (ISO 8601 truncates sub-seconds)
        #expect(abs(decoded.timestamp.timeIntervalSince(resolution.timestamp)) < 1.0)
    }

    // MARK: - SyncJob conflict fields

    @Test("SyncJob default conflict fields")
    func syncJobDefaultConflictFields() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: []
        )
        #expect(job.conflictStrategy == .fail)
        #expect(job.autoAcceptNonDestructive == false)
        #expect(job.conflictFilePath == nil)
        #expect(job.resolveFromPath == nil)
    }

    @Test("SyncJob custom conflict fields")
    func syncJobCustomConflictFields() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: [],
            conflictStrategy: .sourceWins,
            autoAcceptNonDestructive: true,
            conflictFilePath: "/tmp/conflicts.json",
            resolveFromPath: "/tmp/resolved.json"
        )
        #expect(job.conflictStrategy == .sourceWins)
        #expect(job.autoAcceptNonDestructive == true)
        #expect(job.conflictFilePath == "/tmp/conflicts.json")
        #expect(job.resolveFromPath == "/tmp/resolved.json")
    }

    @Test("SyncJob toCloneJob preserves conflict fields")
    func syncJobToCloneJobConflictFields() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: [],
            conflictStrategy: .interactive,
            autoAcceptNonDestructive: true,
            conflictFilePath: "/tmp/c.json",
            resolveFromPath: "/tmp/r.json"
        )
        let cloneJob = job.toCloneJob()
        #expect(cloneJob.conflictStrategy == .interactive)
        #expect(cloneJob.autoAcceptNonDestructive == true)
        #expect(cloneJob.conflictFilePath == "/tmp/c.json")
        #expect(cloneJob.resolveFromPath == "/tmp/r.json")
    }

    // MARK: - CloneJob conflict fields

    @Test("CloneJob default conflict fields")
    func cloneJobDefaultConflictFields() throws {
        let job = CloneJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: []
        )
        #expect(job.conflictStrategy == .fail)
        #expect(job.autoAcceptNonDestructive == false)
        #expect(job.conflictFilePath == nil)
        #expect(job.resolveFromPath == nil)
    }

    @Test("CloneJob custom conflict fields")
    func cloneJobCustomConflictFields() throws {
        let job = CloneJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: [],
            conflictStrategy: .targetWins,
            autoAcceptNonDestructive: true,
            conflictFilePath: "/tmp/c.json",
            resolveFromPath: "/tmp/r.json"
        )
        #expect(job.conflictStrategy == .targetWins)
        #expect(job.autoAcceptNonDestructive == true)
        #expect(job.conflictFilePath == "/tmp/c.json")
        #expect(job.resolveFromPath == "/tmp/r.json")
    }

    // MARK: - Error descriptions

    @Test("Conflict error descriptions")
    func conflictErrorDescriptions() {
        let e1 = PGSchemaEvoError.conflictsDetected(count: 3, destructive: 1)
        #expect(e1.errorDescription?.contains("3 conflict(s)") == true)

        let e2 = PGSchemaEvoError.destructiveActionBlocked(descriptions: ["drop column X"])
        #expect(e2.errorDescription?.contains("drop column X") == true)
        #expect(e2.errorDescription?.contains("--force") == true)

        let e3 = PGSchemaEvoError.conflictFileParseError(path: "/tmp/f.json", underlying: "bad json")
        #expect(e3.errorDescription?.contains("/tmp/f.json") == true)

        let e4 = PGSchemaEvoError.conflictResolutionMismatch(conflictId: "abc-123")
        #expect(e4.errorDescription?.contains("abc-123") == true)
    }
}
