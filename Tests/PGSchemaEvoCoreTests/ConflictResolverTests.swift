import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ConflictResolver Tests")
struct ConflictResolverTests {

    private func makeConflict(
        isDestructive: Bool = false,
        isIrreversible: Bool = false,
        kind: ConflictKind = .divergedDefinition
    ) -> SchemaConflict {
        SchemaConflict(
            objectIdentifier: "table:public.users",
            kind: kind,
            description: "Test conflict",
            sourceSQL: ["ALTER TABLE ...;"],
            targetSQL: ["-- keep target"],
            isDestructive: isDestructive,
            isIrreversible: isIrreversible
        )
    }

    private func makeReport(_ conflicts: [SchemaConflict]) -> ConflictReport {
        ConflictReport(conflicts: conflicts)
    }

    // MARK: - Fail Strategy

    @Test("Fail strategy throws on non-empty report")
    func failThrowsOnConflicts() throws {
        let resolver = ConflictResolver(strategy: .fail, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict()])

        #expect(throws: PGSchemaEvoError.self) {
            try resolver.resolve(report: report)
        }
    }

    @Test("Fail strategy succeeds on empty report")
    func failSucceedsOnEmpty() throws {
        let resolver = ConflictResolver(strategy: .fail, force: false, logger: .init(label: "test"))
        let report = makeReport([])

        let resolutions = try resolver.resolve(report: report)
        #expect(resolutions.isEmpty)
    }

    // MARK: - Source Wins Strategy

    @Test("Source wins resolves all as applySource")
    func sourceWinsResolvesAll() throws {
        let resolver = ConflictResolver(strategy: .sourceWins, force: true, logger: .init(label: "test"))
        let report = makeReport([makeConflict(), makeConflict()])

        let resolutions = try resolver.resolve(report: report)
        #expect(resolutions.count == 2)
        #expect(resolutions.allSatisfy { $0.choice == .applySource })
    }

    @Test("Source wins without force throws on destructive")
    func sourceWinsBlocksDestructive() throws {
        let resolver = ConflictResolver(strategy: .sourceWins, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict(isDestructive: true)])

        #expect(throws: PGSchemaEvoError.self) {
            try resolver.resolve(report: report)
        }
    }

    @Test("Source wins without force allows non-destructive")
    func sourceWinsAllowsNonDestructive() throws {
        let resolver = ConflictResolver(strategy: .sourceWins, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict(isDestructive: false)])

        let resolutions = try resolver.resolve(report: report)
        #expect(resolutions.count == 1)
        #expect(resolutions[0].choice == .applySource)
    }

    @Test("Source wins with force allows destructive")
    func sourceWinsWithForceAllowsDestructive() throws {
        let resolver = ConflictResolver(strategy: .sourceWins, force: true, logger: .init(label: "test"))
        let report = makeReport([makeConflict(isDestructive: true)])

        let resolutions = try resolver.resolve(report: report)
        #expect(resolutions.count == 1)
        #expect(resolutions[0].choice == .applySource)
    }

    // MARK: - Target Wins Strategy

    @Test("Target wins resolves all as keepTarget")
    func targetWinsResolvesAll() throws {
        let resolver = ConflictResolver(strategy: .targetWins, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict(), makeConflict(isDestructive: true)])

        let resolutions = try resolver.resolve(report: report)
        #expect(resolutions.count == 2)
        #expect(resolutions.allSatisfy { $0.choice == .keepTarget })
    }

    // MARK: - Skip Strategy

    @Test("Skip resolves all as skip")
    func skipResolvesAll() throws {
        let resolver = ConflictResolver(strategy: .skip, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict(), makeConflict(isDestructive: true)])

        let resolutions = try resolver.resolve(report: report)
        #expect(resolutions.count == 2)
        #expect(resolutions.allSatisfy { $0.choice == .skip })
    }

    // MARK: - Interactive Strategy (with mock prompter)

    @Test("Interactive resolves via prompter")
    func interactiveResolvesViaPrompter() async throws {
        let resolver = ConflictResolver(strategy: .interactive, force: true, logger: .init(label: "test"))
        let conflicts = [makeConflict(), makeConflict()]
        let report = makeReport(conflicts)
        let prompter = MockConflictPrompter(choices: [.applySource, .keepTarget])

        let resolutions = try await resolver.resolveInteractive(report: report, prompter: prompter)
        #expect(resolutions.count == 2)
        #expect(resolutions[0].choice == .applySource)
        #expect(resolutions[1].choice == .keepTarget)
    }

    @Test("Interactive skips destructive without force")
    func interactiveSkipsDestructiveWithoutForce() async throws {
        let resolver = ConflictResolver(strategy: .interactive, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict(isDestructive: true)])
        let prompter = MockConflictPrompter(choices: [.applySource])

        let resolutions = try await resolver.resolveInteractive(report: report, prompter: prompter)
        #expect(resolutions.count == 1)
        #expect(resolutions[0].choice == .skip) // Forced to skip because destructive without --force
    }

    // MARK: - SQL Collection

    @Test("sqlForResolutions collects only applySource SQL")
    func sqlForResolutions() {
        let c1 = makeConflict()
        let c2 = makeConflict()
        let report = makeReport([c1, c2])
        let resolutions = [
            ConflictResolution(conflictId: c1.id, choice: .applySource),
            ConflictResolution(conflictId: c2.id, choice: .keepTarget)
        ]

        let sql = ConflictResolver.sqlForResolutions(resolutions, report: report)
        #expect(sql.count == 1)
        #expect(sql[0] == "ALTER TABLE ...;")
    }

    @Test("Interactive strategy fallback throws via resolve()")
    func interactiveStrategyFallbackThrows() throws {
        let resolver = ConflictResolver(strategy: .interactive, force: false, logger: .init(label: "test"))
        let report = makeReport([makeConflict()])

        #expect(throws: PGSchemaEvoError.self) {
            try resolver.resolve(report: report)
        }
    }

    @Test("Interactive resolveInteractive on empty report returns empty")
    func interactiveEmptyReport() async throws {
        let resolver = ConflictResolver(strategy: .interactive, force: false, logger: .init(label: "test"))
        let report = makeReport([])
        let prompter = MockConflictPrompter(choices: [])

        let resolutions = try await resolver.resolveInteractive(report: report, prompter: prompter)
        #expect(resolutions.isEmpty)
    }

    @Test("sqlForResolutions returns empty for all keepTarget")
    func sqlForResolutionsEmpty() {
        let c1 = makeConflict()
        let report = makeReport([c1])
        let resolutions = [
            ConflictResolution(conflictId: c1.id, choice: .keepTarget)
        ]

        let sql = ConflictResolver.sqlForResolutions(resolutions, report: report)
        #expect(sql.isEmpty)
    }
}

/// Mock prompter that returns pre-configured choices.
struct MockConflictPrompter: ConflictPrompter, Sendable {
    let choices: [ResolutionChoice]

    func prompt(conflict: SchemaConflict, index: Int, total: Int) async -> ResolutionChoice {
        if index - 1 < choices.count {
            return choices[index - 1]
        }
        return .skip
    }
}
