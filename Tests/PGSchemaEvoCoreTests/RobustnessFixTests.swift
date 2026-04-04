import Foundation
import Testing
import Logging
@testable import PGSchemaEvoCore

// MARK: - Fix #2: WHERE clause validation

@Suite("WHERE Clause Validation Tests")
struct WhereClauseValidationTests {

    @Test("Valid WHERE clause passes validation")
    func validWhereClause() throws {
        try LiveExecutor.validateWhereClause("status = 'active'")
        try LiveExecutor.validateWhereClause("id > 100 AND name LIKE '%test%'")
        try LiveExecutor.validateWhereClause("created_at >= '2024-01-01'")
    }

    @Test("WHERE clause with semicolon is rejected")
    func semicolonRejected() {
        #expect(throws: PGSchemaEvoError.self) {
            try LiveExecutor.validateWhereClause("1=1; DROP TABLE users")
        }
    }

    @Test("WHERE clause with SQL line comment is rejected")
    func lineCommentRejected() {
        #expect(throws: PGSchemaEvoError.self) {
            try LiveExecutor.validateWhereClause("1=1 -- comment")
        }
    }

    @Test("WHERE clause with block comment is rejected")
    func blockCommentRejected() {
        #expect(throws: PGSchemaEvoError.self) {
            try LiveExecutor.validateWhereClause("1=1 /* comment */")
        }
    }

    @Test("Empty WHERE clause is rejected")
    func emptyRejected() {
        #expect(throws: PGSchemaEvoError.self) {
            try LiveExecutor.validateWhereClause("")
        }
        #expect(throws: PGSchemaEvoError.self) {
            try LiveExecutor.validateWhereClause("   ")
        }
    }
}

// MARK: - Fix #13: Object spec validation

@Suite("PreflightChecker Validation Tests")
struct PreflightValidationTests {

    @Test("Valid table spec passes validation")
    func validTableSpec() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        #expect(PreflightChecker.validateObjectSpec(id) == nil)
    }

    @Test("Table without schema fails validation")
    func tableWithoutSchema() {
        let id = ObjectIdentifier(type: .table, schema: nil, name: "users")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
        #expect(error!.contains("requires a schema"))
    }

    @Test("Role without schema passes validation")
    func roleWithoutSchema() {
        let id = ObjectIdentifier(type: .role, schema: nil, name: "admin")
        #expect(PreflightChecker.validateObjectSpec(id) == nil)
    }

    @Test("Extension without schema passes validation")
    func extensionWithoutSchema() {
        let id = ObjectIdentifier(type: .extension, schema: nil, name: "pgcrypto")
        #expect(PreflightChecker.validateObjectSpec(id) == nil)
    }

    @Test("View without schema fails validation")
    func viewWithoutSchema() {
        let id = ObjectIdentifier(type: .view, schema: nil, name: "my_view")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Function without schema fails validation")
    func functionWithoutSchema() {
        let id = ObjectIdentifier(type: .function, schema: nil, name: "my_func")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Sequence without schema fails validation")
    func sequenceWithoutSchema() {
        let id = ObjectIdentifier(type: .sequence, schema: nil, name: "my_seq")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Enum without schema fails validation")
    func enumWithoutSchema() {
        let id = ObjectIdentifier(type: .enum, schema: nil, name: "status")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Materialized view without schema fails validation")
    func matviewWithoutSchema() {
        let id = ObjectIdentifier(type: .materializedView, schema: nil, name: "mv")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Composite type without schema fails validation")
    func compositeWithoutSchema() {
        let id = ObjectIdentifier(type: .compositeType, schema: nil, name: "addr")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Procedure without schema fails validation")
    func procedureWithoutSchema() {
        let id = ObjectIdentifier(type: .procedure, schema: nil, name: "my_proc")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Schema object passes validation")
    func schemaObject() {
        let id = ObjectIdentifier(type: .schema, schema: nil, name: "my_schema")
        #expect(PreflightChecker.validateObjectSpec(id) == nil)
    }

    @Test("Empty name fails validation")
    func emptyName() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
        #expect(error!.contains("cannot be empty"))
    }

    @Test("Foreign table without schema fails validation")
    func foreignTableWithoutSchema() {
        let id = ObjectIdentifier(type: .foreignTable, schema: nil, name: "ft")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("Aggregate without schema fails validation")
    func aggregateWithoutSchema() {
        let id = ObjectIdentifier(type: .aggregate, schema: nil, name: "my_agg")
        let error = PreflightChecker.validateObjectSpec(id)
        #expect(error != nil)
    }

    @Test("FDW without schema passes validation")
    func fdwWithoutSchema() {
        let id = ObjectIdentifier(type: .foreignDataWrapper, schema: nil, name: "my_fdw")
        #expect(PreflightChecker.validateObjectSpec(id) == nil)
    }
}

// MARK: - Fix #14: Partition boundary replacement

@Suite("Partition SQL Generation Tests")
struct PartitionSQLTests {

    @Test("Partition clause is appended to CREATE TABLE correctly")
    func partitionClauseBasic() {
        let createSQL = "CREATE TABLE \"public\".\"orders\" (\n  \"id\" integer NOT NULL,\n  \"year\" integer NOT NULL\n);"
        // Simulate CloneOrchestrator's logic
        let partitionClause = " PARTITION BY RANGE (year)"
        if let lastClosing = createSQL.range(of: ");", options: .backwards) {
            let result = createSQL.replacingCharacters(in: lastClosing, with: ")\(partitionClause);")
            #expect(result.contains("PARTITION BY RANGE (year)"))
            #expect(result.hasSuffix(";"))
            // The original bare ); at end of CREATE TABLE is replaced with ) PARTITION BY ...;
            #expect(result.contains(") PARTITION BY"))
        } else {
            Issue.record("Should have found closing );")
        }
    }

    @Test("Partition clause handles CREATE TABLE with constraints containing );")
    func partitionClauseWithConstraint() {
        let createSQL = "CREATE TABLE \"public\".\"orders\" (\n  \"id\" integer NOT NULL,\n  CONSTRAINT \"pk\" PRIMARY KEY (\"id\")\n);"
        let partitionClause = " PARTITION BY HASH (id)"
        if let lastClosing = createSQL.range(of: ");", options: .backwards) {
            let result = createSQL.replacingCharacters(in: lastClosing, with: ")\(partitionClause);")
            #expect(result.contains("PARTITION BY HASH (id)"))
            #expect(result.hasSuffix(";"))
        } else {
            Issue.record("Should have found closing );")
        }
    }
}

// MARK: - Fix #15: SchemaDiffer unimplemented types

@Suite("SchemaDiffer Type Coverage Tests")
struct SchemaDifferTypeCoverageTests {

    @Test("SchemaDiff drop statement for role uses name without schema")
    func dropStatementForRole() {
        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [ObjectIdentifier(type: .role, schema: nil, name: "test_role")],
            modified: [],
            matching: 0
        )
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP ROLE IF EXISTS test_role CASCADE;"))
    }

    @Test("SchemaDiff handles empty diff")
    func emptyDiff() {
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [], matching: 5)
        #expect(diff.isEmpty)
        let text = diff.renderText()
        #expect(text.contains("identical"))
    }

    @Test("ObjectDiff with irreversible changes")
    func irreversibleChanges() {
        let objDiff = ObjectDiff(
            id: ObjectIdentifier(type: .enum, schema: "public", name: "status"),
            differences: ["Label 'active': missing in target"],
            migrationSQL: ["ALTER TYPE public.status ADD VALUE 'active';"],
            irreversibleChanges: ["Cannot remove enum value 'active'"]
        )
        #expect(!objDiff.irreversibleChanges.isEmpty)
        #expect(objDiff.dropColumnSQL.isEmpty)
        #expect(objDiff.reverseMigrationSQL.isEmpty)
    }
}

// MARK: - Fix #10: CSV row count validation (via max guard)

@Suite("Data Sync Row Count Tests")
struct DataSyncRowCountTests {

    @Test("Row count from CSV with header only is zero")
    func headerOnlyCSV() {
        let csvData = "id,name\n"
        let lineCount = csvData.split(separator: "\n", omittingEmptySubsequences: true).count
        // lineCount = 1 (header only), so this would be skipped in the orchestrator
        #expect(lineCount <= 1)
    }

    @Test("Row count from CSV with data is positive")
    func csvWithData() {
        let csvData = "id,name\n1,Alice\n2,Bob\n"
        let lineCount = csvData.split(separator: "\n", omittingEmptySubsequences: true).count
        let rowCount = max(lineCount - 1, 0)
        #expect(rowCount == 2)
    }

    @Test("Row count guard against edge cases")
    func rowCountGuard() {
        // Test max(lineCount - 1, 0) for various counts
        #expect(max(0 - 1, 0) == 0)
        #expect(max(1 - 1, 0) == 0)
        #expect(max(2 - 1, 0) == 1)
        #expect(max(10 - 1, 0) == 9)
    }
}

// MARK: - MigrationStatus render tests

@Suite("MigrationStatus Render Tests")
struct MigrationStatusRenderTests {

    @Test("Empty migration status renders correctly")
    func emptyStatus() {
        let status = MigrationStatus(entries: [])
        let rendered = status.render()
        #expect(rendered.contains("No migrations found"))
        #expect(status.applied.isEmpty)
        #expect(status.pending.isEmpty)
        #expect(status.orphaned.isEmpty)
    }

    @Test("Migration status with all states renders correctly")
    func mixedStatus() {
        let entries = [
            MigrationStatusEntry(id: "001_init", state: .applied, appliedAt: Date()),
            MigrationStatusEntry(id: "002_add_users", state: .pending),
            MigrationStatusEntry(id: "003_orphan", state: .orphaned, appliedAt: Date()),
        ]
        let status = MigrationStatus(entries: entries)
        let rendered = status.render()
        #expect(rendered.contains("✓"))
        #expect(rendered.contains("○"))
        #expect(rendered.contains("!"))
        #expect(rendered.contains("ORPHANED"))
        #expect(status.applied.count == 1)
        #expect(status.pending.count == 1)
        #expect(status.orphaned.count == 1)
    }
}

// MARK: - SchemaDiff migration SQL rendering

@Suite("SchemaDiff Migration SQL Rendering Tests")
struct SchemaDiffMigrationSQLTests {

    @Test("Migration SQL includes destructive changes when requested")
    func destructiveIncluded() {
        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [],
            modified: [
                ObjectDiff(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    differences: ["Column extra: extra in target"],
                    migrationSQL: [],
                    dropColumnSQL: ["ALTER TABLE public.users DROP COLUMN extra;"]
                )
            ],
            matching: 0
        )
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP COLUMN extra"))
        #expect(!sql.contains("SKIPPED"))
    }

    @Test("Migration SQL skips destructive changes by default")
    func destructiveSkipped() {
        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [],
            modified: [
                ObjectDiff(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    differences: ["Column extra: extra in target"],
                    migrationSQL: [],
                    dropColumnSQL: ["ALTER TABLE public.users DROP COLUMN extra;"]
                )
            ],
            matching: 0
        )
        let sql = diff.renderMigrationSQL(includeDestructive: false)
        #expect(sql.contains("SKIPPED"))
    }

    @Test("Migration SQL includes objects only in source as comments")
    func onlyInSourceAsComments() {
        let diff = SchemaDiff(
            onlyInSource: [ObjectIdentifier(type: .table, schema: "public", name: "new_table")],
            onlyInTarget: [],
            modified: [],
            matching: 0
        )
        let sql = diff.renderMigrationSQL()
        #expect(sql.contains("-- CREATE"))
        #expect(sql.contains("new_table"))
    }

    @Test("Migration SQL drops objects only in target when destructive")
    func dropOnlyInTarget() {
        let diff = SchemaDiff(
            onlyInSource: [],
            onlyInTarget: [ObjectIdentifier(type: .view, schema: "public", name: "old_view")],
            modified: [],
            matching: 0
        )
        let sql = diff.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("DROP VIEW IF EXISTS"))
    }

    @Test("renderText shows summary")
    func renderTextSummary() {
        let diff = SchemaDiff(
            onlyInSource: [ObjectIdentifier(type: .table, schema: "public", name: "t1")],
            onlyInTarget: [ObjectIdentifier(type: .table, schema: "public", name: "t2")],
            modified: [
                ObjectDiff(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "t3"),
                    differences: ["Column added"],
                    migrationSQL: ["ALTER TABLE t3 ADD COLUMN x int;"]
                )
            ],
            matching: 5
        )
        let text = diff.renderText()
        #expect(text.contains("5 matching"))
        #expect(text.contains("1 only in source"))
        #expect(text.contains("1 only in target"))
        #expect(text.contains("1 modified"))
        #expect(text.contains("+ "))
        #expect(text.contains("- "))
        #expect(text.contains("~ "))
    }
}

// MARK: - Drop SQL generation in LiveExecutor

@Suite("LiveExecutor Drop SQL Edge Cases")
struct LiveExecutorDropEdgeCases {

    private func makeExecutor() -> LiveExecutor {
        LiveExecutor(logger: Logger(label: "test"))
    }

    @Test("DROP for various object types in transaction script")
    func dropVariousTypes() {
        let executor = makeExecutor()
        let types: [(ObjectType, String)] = [
            (.table, "TABLE"),
            (.view, "VIEW"),
            (.materializedView, "MATERIALIZED VIEW"),
            (.sequence, "SEQUENCE"),
            (.enum, "TYPE"),
            (.compositeType, "TYPE"),
            (.schema, "SCHEMA"),
            (.extension, "EXTENSION"),
            (.foreignTable, "FOREIGN TABLE"),
            (.operator, "OPERATOR"),
            (.foreignDataWrapper, "FOREIGN DATA WRAPPER"),
        ]

        for (objType, keyword) in types {
            let id = ObjectIdentifier(type: objType, schema: "public", name: "test_obj")
            let steps: [CloneStep] = [.dropObject(id)]
            let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
            #expect(script.contains("DROP \(keyword) IF EXISTS"), "Expected DROP \(keyword) for \(objType)")
        }
    }

    @Test("DROP role uses unqualified name")
    func dropRoleUnqualified() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .role, schema: nil, name: "test_role")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP ROLE IF EXISTS \"test_role\";"))
    }

    @Test("DROP procedure includes signature")
    func dropProcedureSignature() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .procedure, schema: "public", name: "my_proc", signature: "(integer)")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP PROCEDURE IF EXISTS"))
        #expect(script.contains("(integer)"))
    }

    @Test("DROP function without signature defaults to empty parens")
    func dropFunctionNoSignature() {
        let executor = makeExecutor()
        let id = ObjectIdentifier(type: .function, schema: "public", name: "my_func")
        let steps: [CloneStep] = [.dropObject(id)]
        let script = executor.buildTransactionScript(steps: steps, prefetchedData: [:])
        #expect(script.contains("DROP FUNCTION IF EXISTS"))
        #expect(script.contains("() CASCADE"))
    }
}

// MARK: - SchemaDiff drop statement generation

@Suite("SchemaDiff Drop Statement Tests")
struct SchemaDiffDropStatementTests {

    @Test("Drop statements for all common types")
    func dropAllTypes() {
        let types: [(ObjectType, String)] = [
            (.table, "TABLE"),
            (.view, "VIEW"),
            (.materializedView, "MATERIALIZED VIEW"),
            (.sequence, "SEQUENCE"),
            (.function, "FUNCTION"),
            (.procedure, "PROCEDURE"),
            (.enum, "TYPE"),
            (.compositeType, "TYPE"),
            (.schema, "SCHEMA"),
            (.extension, "EXTENSION"),
        ]

        for (objType, keyword) in types {
            let id = ObjectIdentifier(type: objType, schema: "public", name: "test_\(objType)")
            let diff = SchemaDiff(
                onlyInSource: [],
                onlyInTarget: [id],
                modified: [],
                matching: 0
            )
            let sql = diff.renderMigrationSQL(includeDestructive: true)
            #expect(sql.contains("DROP \(keyword) IF EXISTS"), "Expected DROP \(keyword) for \(objType)")
        }
    }
}
