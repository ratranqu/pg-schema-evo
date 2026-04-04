import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ConflictReport Tests")
struct ConflictReportTests {

    @Test("Empty report renders correctly")
    func emptyReport() {
        let report = ConflictReport(conflicts: [])
        #expect(report.isEmpty)
        #expect(report.count == 0)
        #expect(report.destructiveConflicts.isEmpty)
        #expect(report.nonDestructiveConflicts.isEmpty)
        #expect(report.irreversibleConflicts.isEmpty)
        #expect(report.renderText().contains("No conflicts detected"))
    }

    @Test("Report with conflicts renders text")
    func reportRendersText() {
        let conflicts = [
            SchemaConflict(
                objectIdentifier: "table:public.users",
                kind: .extraInTarget,
                description: "Column legacy: extra in target",
                sourceSQL: ["DROP COLUMN ..."],
                isDestructive: true,
                detail: "data loss risk"
            ),
            SchemaConflict(
                objectIdentifier: "table:public.users",
                kind: .divergedDefinition,
                description: "Column age: type changed",
                sourceSQL: ["ALTER COLUMN ..."]
            )
        ]
        let report = ConflictReport(conflicts: conflicts)

        #expect(!report.isEmpty)
        #expect(report.count == 2)
        #expect(report.destructiveConflicts.count == 1)
        #expect(report.nonDestructiveConflicts.count == 1)

        let text = report.renderText()
        #expect(text.contains("2"))
        #expect(text.contains("1 destructive"))
        #expect(text.contains("DESTRUCTIVE"))
        #expect(text.contains("table:public.users"))
        #expect(text.contains("data loss risk"))
    }

    @Test("Irreversible conflicts filtered correctly")
    func irreversibleFilter() {
        let conflicts = [
            SchemaConflict(
                objectIdentifier: "enum:public.status",
                kind: .irreversibleChange,
                description: "Cannot remove enum value",
                sourceSQL: [],
                isIrreversible: true
            ),
            SchemaConflict(
                objectIdentifier: "table:public.users",
                kind: .divergedDefinition,
                description: "Column changed",
                sourceSQL: ["ALTER ..."]
            )
        ]
        let report = ConflictReport(conflicts: conflicts)
        #expect(report.irreversibleConflicts.count == 1)
        #expect(report.irreversibleConflicts[0].kind == .irreversibleChange)
    }
}
