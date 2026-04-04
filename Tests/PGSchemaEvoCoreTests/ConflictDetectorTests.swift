import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ConflictDetector Tests")
struct ConflictDetectorTests {
    let detector = ConflictDetector(logger: .init(label: "test"))

    @Test("Empty diff produces no conflicts")
    func emptyDiff() {
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [], matching: 5)
        let report = detector.detect(from: diff)
        #expect(report.isEmpty)
        #expect(report.count == 0)
    }

    @Test("Objects only in target produce destructive conflicts")
    func objectsOnlyInTarget() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "extra_table")
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [id], modified: [], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 1)
        #expect(report.conflicts[0].kind == .objectOnlyInTarget)
        #expect(report.conflicts[0].isDestructive == true)
        #expect(report.conflicts[0].objectIdentifier == id.description)
        #expect(report.conflicts[0].sourceSQL.first?.contains("DROP TABLE") == true)
    }

    @Test("Objects only in source produce no conflicts")
    func objectsOnlyInSource() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "new_table")
        let diff = SchemaDiff(onlyInSource: [id], onlyInTarget: [], modified: [], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.isEmpty)
    }

    @Test("Modified object with only safe migration SQL produces no conflicts")
    func safeMigrationNoConflict() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["Column age: type integer -> bigint"],
            migrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE bigint;"],
            reverseMigrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE integer;"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.isEmpty)
    }

    @Test("Modified object with dropColumnSQL produces extraInTarget destructive conflicts")
    func destructiveDropColumn() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["Column legacy: extra in target (not in source)"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" DROP COLUMN \"legacy\";"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" ADD COLUMN \"legacy\" text;"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 1)
        #expect(report.conflicts[0].kind == .extraInTarget)
        #expect(report.conflicts[0].isDestructive == true)
        #expect(report.destructiveConflicts.count == 1)
    }

    @Test("Modified object with both safe and destructive changes produces only destructive conflict")
    func mixedChanges() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: [
                "Column age: type integer -> bigint",
                "Column legacy: extra in target (not in source)"
            ],
            migrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE bigint;"],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" DROP COLUMN \"legacy\";"],
            reverseMigrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE integer;"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" ADD COLUMN \"legacy\" text;"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        // Only the destructive change is a conflict; safe migration is always applied
        #expect(report.count == 1)
        #expect(report.destructiveConflicts.count == 1)
        #expect(report.conflicts[0].kind == .extraInTarget)
    }

    @Test("Irreversible changes produce irreversibleChange conflicts")
    func irreversibleChanges() {
        let id = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["Label 'old_value': extra in target (cannot remove enum values in PostgreSQL)"],
            migrationSQL: [],
            irreversibleChanges: ["Cannot remove enum value 'old_value' from public.status (PostgreSQL limitation)"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.irreversibleConflicts.count == 1)
        #expect(report.irreversibleConflicts[0].kind == .irreversibleChange)
        #expect(report.irreversibleConflicts[0].isIrreversible == true)
    }

    @Test("Multiple objects only in target")
    func multipleTargetOnly() {
        let ids = [
            ObjectIdentifier(type: .table, schema: "public", name: "t1"),
            ObjectIdentifier(type: .view, schema: "public", name: "v1"),
            ObjectIdentifier(type: .sequence, schema: "public", name: "s1")
        ]
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: ids, modified: [], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 3)
        #expect(report.destructiveConflicts.count == 3)
    }

    @Test("Extra constraint in target is destructive")
    func extraConstraint() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["Constraint users_email_key: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" DROP CONSTRAINT \"users_email_key\";"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" ADD CONSTRAINT \"users_email_key\" UNIQUE (email);"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 1)
        #expect(report.conflicts[0].isDestructive == true)
    }

    @Test("Extra index in target is destructive")
    func extraIndex() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["Index idx_users_email: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["DROP INDEX \"public\".\"idx_users_email\";"],
            reverseDropColumnSQL: ["CREATE INDEX \"idx_users_email\" ON \"public\".\"users\" (email);"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 1)
        #expect(report.conflicts[0].isDestructive == true)
    }

    @Test("Drop statement for various object types")
    func dropStatementObjectTypes() {
        let types: [(ObjectType, String)] = [
            (.materializedView, "MATERIALIZED VIEW"),
            (.function, "FUNCTION"),
            (.procedure, "PROCEDURE"),
            (.enum, "TYPE"),
            (.compositeType, "TYPE"),
            (.schema, "SCHEMA"),
            (.extension, "EXTENSION"),
        ]
        for (objType, keyword) in types {
            let id = ObjectIdentifier(type: objType, schema: "public", name: "test_obj")
            let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [id], modified: [], matching: 0)
            let report = detector.detect(from: diff)
            #expect(report.count == 1)
            #expect(report.conflicts[0].sourceSQL.first?.contains(keyword) == true,
                    "Expected \(keyword) in DROP for \(objType)")
        }
    }

    @Test("Destructive diffs exceed dropColumnSQL count")
    func destructiveDiffsExceedDropSQL() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: [
                "Column a: extra in target",
                "Column b: extra in target"
            ],
            migrationSQL: [],
            dropColumnSQL: ["DROP COLUMN a;"],  // Only 1 SQL for 2 diffs
            reverseDropColumnSQL: []
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 2)
        // Second conflict should have empty SQL since index exceeds count
        #expect(report.conflicts[1].sourceSQL.isEmpty)
        #expect(report.conflicts[1].targetSQL.isEmpty)
    }

    @Test("RLS forced on target but not source is detected")
    func rlsForcedExtraInTarget() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["RLS: forced on target but not on source"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" NO FORCE ROW LEVEL SECURITY;"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" FORCE ROW LEVEL SECURITY;"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 1)
        #expect(report.conflicts[0].isDestructive == true)
    }

    @Test("RLS enabled on target but not source is detected")
    func rlsExtraInTarget() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let objDiff = ObjectDiff(
            id: id,
            differences: ["RLS: enabled on target but not on source"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" DISABLE ROW LEVEL SECURITY;"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" ENABLE ROW LEVEL SECURITY;"]
        )
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [objDiff], matching: 0)
        let report = detector.detect(from: diff)
        #expect(report.count == 1)
        #expect(report.conflicts[0].isDestructive == true)
    }
}
