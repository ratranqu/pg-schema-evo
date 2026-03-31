import Testing
import Logging
@testable import PGSchemaEvoCore

@Suite("SchemaDiffer Tests")
struct SchemaDifferTests {

    @Test("Empty diff when schemas are identical")
    func identicalSchemas() async throws {
        let mock = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "orders"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: mock, target: mock)

        #expect(result.isEmpty)
        #expect(result.renderText().contains("identical"))
    }

    @Test("Detects objects only in source")
    func onlyInSource() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "orders"),
        ])
        let target = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.onlyInSource.count == 1)
        #expect(result.onlyInSource[0].name == "orders")
        #expect(result.onlyInTarget.isEmpty)
    }

    @Test("Detects objects only in target")
    func onlyInTarget() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
        ])
        let target = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "legacy"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.onlyInTarget.count == 1)
        #expect(result.onlyInTarget[0].name == "legacy")
    }

    @Test("Text diff format includes summary")
    func textDiffFormat() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
            ObjectIdentifier(type: .table, schema: "public", name: "new_table"),
        ])
        let target = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "users"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let text = result.renderText()
        #expect(text.contains("only in source"))
        #expect(text.contains("Summary:"))
    }

    @Test("SQL migration output includes BEGIN/COMMIT")
    func migrationSQLFormat() async throws {
        let source = MockDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "new_table"),
        ])
        let target = MockDiffIntrospector(objects: [])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sql = result.renderMigrationSQL()
        #expect(sql.contains("BEGIN;"))
        #expect(sql.contains("COMMIT;"))
    }

    // MARK: - Tests using ConfigurableDiffIntrospector

    @Test("Table column type difference detected")
    func tableColumnTypeDifference() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "name", dataType: "text", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "name", dataType: "varchar", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("type") && $0.contains("text") })
    }

    @Test("Table column nullability difference")
    func tableColumnNullabilityDifference() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "email", dataType: "text", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "email", dataType: "text", isNullable: true, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("nullability") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("SET NOT NULL") })
    }

    @Test("Table column default difference")
    func tableColumnDefaultDifference() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "status", dataType: "text", isNullable: false, columnDefault: "'active'", ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "status", dataType: "text", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("default") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("SET DEFAULT") })
    }

    @Test("Table missing column in target")
    func tableMissingColumnInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "bio", dataType: "text", isNullable: true, ordinalPosition: 2),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("bio") && $0.contains("missing in target") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("ADD COLUMN") })
    }

    @Test("Table extra column in target")
    func tableExtraColumnInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "legacy_field", dataType: "text", isNullable: true, ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("legacy_field") && $0.contains("extra in target") })
    }

    @Test("Table constraint difference")
    func tableConstraintDifference() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            constraints: [
                ConstraintInfo(name: "users_pkey", type: .primaryKey, definition: "PRIMARY KEY (id)"),
                ConstraintInfo(name: "users_email_unique", type: .unique, definition: "UNIQUE (email)"),
            ]
        )
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            constraints: [
                ConstraintInfo(name: "users_pkey", type: .primaryKey, definition: "PRIMARY KEY (id)"),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("users_email_unique") && $0.contains("missing in target") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("ADD CONSTRAINT") })
    }

    @Test("Table index difference")
    func tableIndexDifference() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            indexes: [
                IndexInfo(name: "idx_users_email", definition: "CREATE INDEX idx_users_email ON public.users USING btree (email)", isUnique: false, isPrimary: false),
            ]
        )
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("idx_users_email") && $0.contains("missing in target") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("CREATE INDEX") })
    }

    @Test("View definition difference")
    func viewDefinitionDifference() async throws {
        let viewId = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let source = ConfigurableDiffIntrospector(objects: [viewId])
        source.views[viewId] = ViewMetadata(id: viewId, definition: "SELECT id, name FROM users WHERE active = true")
        let target = ConfigurableDiffIntrospector(objects: [viewId])
        target.views[viewId] = ViewMetadata(id: viewId, definition: "SELECT id FROM users WHERE active = true")

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("Definition differs") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("CREATE OR REPLACE VIEW") })
    }

    @Test("Sequence property differences")
    func sequencePropertyDifferences() async throws {
        let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "users_id_seq")
        let source = ConfigurableDiffIntrospector(objects: [seqId])
        source.sequences[seqId] = SequenceMetadata(id: seqId, increment: 5, cacheSize: 10)
        let target = ConfigurableDiffIntrospector(objects: [seqId])
        target.sequences[seqId] = SequenceMetadata(id: seqId, increment: 1, cacheSize: 1)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("INCREMENT") })
        #expect(result.modified[0].differences.contains { $0.contains("CACHE") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("ALTER SEQUENCE") && $0.contains("INCREMENT BY 5") && $0.contains("CACHE 10") })
    }

    @Test("Enum label differences")
    func enumLabelDifferences() async throws {
        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let source = ConfigurableDiffIntrospector(objects: [enumId])
        source.enums[enumId] = EnumMetadata(id: enumId, labels: ["active", "inactive", "suspended"])
        let target = ConfigurableDiffIntrospector(objects: [enumId])
        target.enums[enumId] = EnumMetadata(id: enumId, labels: ["active", "inactive", "archived"])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("suspended") && $0.contains("missing in target") })
        #expect(result.modified[0].differences.contains { $0.contains("archived") && $0.contains("extra in target") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("ADD VALUE") && $0.contains("suspended") })
    }

    @Test("Function definition difference")
    func functionDefinitionDifference() async throws {
        let funcId = ObjectIdentifier(type: .function, schema: "public", name: "calculate_total")
        let source = ConfigurableDiffIntrospector(objects: [funcId])
        source.functions[funcId] = FunctionMetadata(
            id: funcId,
            definition: "CREATE OR REPLACE FUNCTION public.calculate_total() RETURNS integer AS $$ SELECT sum(amount) FROM orders $$ LANGUAGE sql"
        )
        let target = ConfigurableDiffIntrospector(objects: [funcId])
        target.functions[funcId] = FunctionMetadata(
            id: funcId,
            definition: "CREATE OR REPLACE FUNCTION public.calculate_total() RETURNS integer AS $$ SELECT sum(price) FROM orders $$ LANGUAGE sql"
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("Function definition differs") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("calculate_total") })
    }

    @Test("Migration SQL contains ALTER TABLE for table diffs")
    func migrationSQLContainsAlterTable() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "total", dataType: "numeric", isNullable: false, ordinalPosition: 2),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "total", dataType: "integer", isNullable: false, ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sql = result.renderMigrationSQL()
        #expect(sql.contains("ALTER TABLE"))
        #expect(sql.contains("BEGIN;"))
        #expect(sql.contains("COMMIT;"))
    }

    @Test("Modified objects section in text output")
    func modifiedObjectsTextOutput() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "bigint", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let text = result.renderText()
        #expect(text.contains("Modified objects"))
        #expect(text.contains("~"))
        #expect(text.contains("Summary:"))
        #expect(text.contains("1 modified"))
    }

    @Test("Filter by type only shows matching types")
    func filterByType() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let viewId = ObjectIdentifier(type: .view, schema: "public", name: "active_users")
        let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "users_id_seq")

        let source = ConfigurableDiffIntrospector(objects: [tableId, viewId, seqId])
        let target = ConfigurableDiffIntrospector(objects: [])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target, types: [.table])

        #expect(result.onlyInSource.count == 1)
        #expect(result.onlyInSource[0].type == .table)
        #expect(result.onlyInSource[0].name == "users")
    }

    @Test("Filter by schema only shows matching schema")
    func filterBySchema() async throws {
        let publicTable = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let auditTable = ObjectIdentifier(type: .table, schema: "audit", name: "logs")

        let source = ConfigurableDiffIntrospector(objects: [publicTable, auditTable])
        let target = ConfigurableDiffIntrospector(objects: [])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target, schema: "audit")

        #expect(result.onlyInSource.count == 1)
        #expect(result.onlyInSource[0].schema == "audit")
        #expect(result.onlyInSource[0].name == "logs")
    }

    // MARK: - Additional coverage for untested code paths

    @Test("Materialized view definition difference detected")
    func materializedViewDefinitionDifference() async throws {
        let mvId = ObjectIdentifier(type: .materializedView, schema: "public", name: "daily_stats")
        let source = ConfigurableDiffIntrospector(objects: [mvId])
        source.materializedViews[mvId] = MaterializedViewMetadata(
            id: mvId,
            definition: "SELECT date, count(*) FROM events GROUP BY date"
        )
        let target = ConfigurableDiffIntrospector(objects: [mvId])
        target.materializedViews[mvId] = MaterializedViewMetadata(
            id: mvId,
            definition: "SELECT date, sum(amount) FROM events GROUP BY date"
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("Definition differs") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("CREATE OR REPLACE MATERIALIZED VIEW") })
    }

    @Test("Materialized view with identical definitions returns no diff")
    func materializedViewIdentical() async throws {
        let mvId = ObjectIdentifier(type: .materializedView, schema: "public", name: "daily_stats")
        let definition = "SELECT date, count(*) FROM events GROUP BY date"
        let source = ConfigurableDiffIntrospector(objects: [mvId])
        source.materializedViews[mvId] = MaterializedViewMetadata(id: mvId, definition: definition)
        let target = ConfigurableDiffIntrospector(objects: [mvId])
        target.materializedViews[mvId] = MaterializedViewMetadata(id: mvId, definition: definition)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    @Test("Table column default dropped when source has no default")
    func tableColumnDefaultDropped() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "status", dataType: "text", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "status", dataType: "text", isNullable: false, columnDefault: "'active'", ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("default") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("DROP DEFAULT") })
    }

    @Test("Table column nullability changed from NOT NULL to nullable generates DROP NOT NULL")
    func tableColumnNullabilityDropNotNull() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "email", dataType: "text", isNullable: true, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "email", dataType: "text", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].migrationSQL.contains { $0.contains("DROP NOT NULL") })
    }

    @Test("Table missing column with NOT NULL and default generates correct ADD COLUMN")
    func tableMissingColumnNotNullWithDefault() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "role", dataType: "text", isNullable: false, columnDefault: "'user'", ordinalPosition: 2),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        let sql = result.modified[0].migrationSQL.joined(separator: " ")
        #expect(sql.contains("ADD COLUMN"))
        #expect(sql.contains("NOT NULL"))
        #expect(sql.contains("DEFAULT 'user'"))
    }

    @Test("Extra constraint in target is detected")
    func tableExtraConstraintInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            constraints: []
        )
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            constraints: [
                ConstraintInfo(name: "orders_legacy_check", type: .check, definition: "CHECK (amount > 0)"),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("orders_legacy_check") && $0.contains("extra in target") })
    }

    @Test("Extra index in target is detected")
    func tableExtraIndexInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            indexes: [
                IndexInfo(name: "idx_orders_legacy", definition: "CREATE INDEX idx_orders_legacy ON public.orders USING btree (legacy_col)", isUnique: false, isPrimary: false),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("idx_orders_legacy") && $0.contains("extra in target") })
    }

    @Test("Sequence min/max/cycle differences detected")
    func sequenceMinMaxCycleDifferences() async throws {
        let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "counter_seq")
        let source = ConfigurableDiffIntrospector(objects: [seqId])
        source.sequences[seqId] = SequenceMetadata(
            id: seqId,
            increment: 1,
            minValue: 10,
            maxValue: 1000,
            cacheSize: 1,
            isCycled: true
        )
        let target = ConfigurableDiffIntrospector(objects: [seqId])
        target.sequences[seqId] = SequenceMetadata(
            id: seqId,
            increment: 1,
            minValue: 1,
            maxValue: Int64.max,
            cacheSize: 1,
            isCycled: false
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("MIN") })
        #expect(result.modified[0].differences.contains { $0.contains("MAX") })
        #expect(result.modified[0].differences.contains { $0.contains("CYCLE") })
        let sql = result.modified[0].migrationSQL.joined(separator: " ")
        #expect(sql.contains("MINVALUE 10"))
        #expect(sql.contains("MAXVALUE 1000"))
        #expect(sql.contains("CYCLE"))
    }

    @Test("Sequence with identical properties returns no diff")
    func sequenceIdentical() async throws {
        let seqId = ObjectIdentifier(type: .sequence, schema: "public", name: "id_seq")
        let source = ConfigurableDiffIntrospector(objects: [seqId])
        source.sequences[seqId] = SequenceMetadata(id: seqId, increment: 1, cacheSize: 1)
        let target = ConfigurableDiffIntrospector(objects: [seqId])
        target.sequences[seqId] = SequenceMetadata(id: seqId, increment: 1, cacheSize: 1)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    @Test("Enum label added at first position uses no AFTER clause")
    func enumLabelAddedAtFirstPosition() async throws {
        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "priority")
        let source = ConfigurableDiffIntrospector(objects: [enumId])
        source.enums[enumId] = EnumMetadata(id: enumId, labels: ["critical", "high", "low"])
        let target = ConfigurableDiffIntrospector(objects: [enumId])
        target.enums[enumId] = EnumMetadata(id: enumId, labels: ["high", "low"])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        // "critical" is at index 0, so no AFTER clause
        let addValueSQL = result.modified[0].migrationSQL.first { $0.contains("critical") }
        #expect(addValueSQL != nil)
        #expect(!addValueSQL!.contains("AFTER"))
    }

    @Test("Enum with identical labels returns no diff")
    func enumIdentical() async throws {
        let enumId = ObjectIdentifier(type: .enum, schema: "public", name: "status")
        let source = ConfigurableDiffIntrospector(objects: [enumId])
        source.enums[enumId] = EnumMetadata(id: enumId, labels: ["a", "b", "c"])
        let target = ConfigurableDiffIntrospector(objects: [enumId])
        target.enums[enumId] = EnumMetadata(id: enumId, labels: ["a", "b", "c"])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    @Test("Function with identical definitions returns no diff")
    func functionIdentical() async throws {
        let funcId = ObjectIdentifier(type: .function, schema: "public", name: "my_func")
        let def = "CREATE OR REPLACE FUNCTION public.my_func() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql"
        let source = ConfigurableDiffIntrospector(objects: [funcId])
        source.functions[funcId] = FunctionMetadata(id: funcId, definition: def)
        let target = ConfigurableDiffIntrospector(objects: [funcId])
        target.functions[funcId] = FunctionMetadata(id: funcId, definition: def)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    @Test("compareObject returns nil for unsupported types like schema")
    func unsupportedTypeReturnsNil() async throws {
        let schemaId = ObjectIdentifier(type: .schema, name: "custom_schema")
        let source = ConfigurableDiffIntrospector(objects: [schemaId])
        let target = ConfigurableDiffIntrospector(objects: [schemaId])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        // Schema type goes through default branch which returns nil, so no modifications
        #expect(result.modified.isEmpty)
        #expect(result.matching == 1)
    }

    @Test("renderText shows onlyInTarget section")
    func renderTextOnlyInTarget() async throws {
        let source = ConfigurableDiffIntrospector(objects: [])
        let target = ConfigurableDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "orphan"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let text = result.renderText()
        #expect(text.contains("Objects only in target"))
        #expect(text.contains("- table:public.orphan"))
    }

    @Test("renderMigrationSQL includes modified objects and comments for missing")
    func renderMigrationSQLModifiedAndMissing() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let newTableId = ObjectIdentifier(type: .table, schema: "public", name: "new_table")

        let source = ConfigurableDiffIntrospector(objects: [tableId, newTableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "bigint", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sql = result.renderMigrationSQL()
        #expect(sql.contains("-- Modify table:public.users"))
        #expect(sql.contains("ALTER TABLE"))
        #expect(sql.contains("-- Objects missing in target"))
        #expect(sql.contains("-- CREATE table:public.new_table"))
    }

    @Test("SchemaDiff isEmpty is true when no differences")
    func schemaDiffIsEmpty() {
        let diff = SchemaDiff(onlyInSource: [], onlyInTarget: [], modified: [], matching: 5)
        #expect(diff.isEmpty)
    }

    @Test("SchemaDiff isEmpty is false when there are differences")
    func schemaDiffIsNotEmpty() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let diff = SchemaDiff(onlyInSource: [id], onlyInTarget: [], modified: [], matching: 0)
        #expect(!diff.isEmpty)
    }

    @Test("View with identical definitions returns no diff")
    func viewIdentical() async throws {
        let viewId = ObjectIdentifier(type: .view, schema: "public", name: "v")
        let def = "SELECT id, name FROM users"
        let source = ConfigurableDiffIntrospector(objects: [viewId])
        source.views[viewId] = ViewMetadata(id: viewId, definition: def)
        let target = ConfigurableDiffIntrospector(objects: [viewId])
        target.views[viewId] = ViewMetadata(id: viewId, definition: def)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    @Test("Procedure definition difference detected")
    func procedureDefinitionDifference() async throws {
        let procId = ObjectIdentifier(type: .procedure, schema: "public", name: "cleanup")
        let source = ConfigurableDiffIntrospector(objects: [procId])
        source.functions[procId] = FunctionMetadata(
            id: procId,
            definition: "CREATE OR REPLACE PROCEDURE public.cleanup() AS $$ DELETE FROM logs WHERE age > 30 $$ LANGUAGE sql"
        )
        let target = ConfigurableDiffIntrospector(objects: [procId])
        target.functions[procId] = FunctionMetadata(
            id: procId,
            definition: "CREATE OR REPLACE PROCEDURE public.cleanup() AS $$ DELETE FROM logs WHERE age > 90 $$ LANGUAGE sql"
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("Function definition differs") })
    }

    @Test("Table with identical columns returns no diff")
    func tableIdentical() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let meta = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "name", dataType: "text", isNullable: true, ordinalPosition: 2),
        ])
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = meta
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = meta

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    @Test("Multiple differences in a single table are all captured")
    func tableMultipleDifferences() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "products")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "bigint", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "price", dataType: "numeric", isNullable: false, columnDefault: "0", ordinalPosition: 2),
            ColumnInfo(name: "sku", dataType: "text", isNullable: true, ordinalPosition: 3),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "price", dataType: "numeric", isNullable: true, ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        // Should detect: id type change, price nullability change, price default change, sku missing
        #expect(result.modified[0].differences.count >= 3)
    }
}

/// Mock introspector for diff tests that returns canned object lists.
private final class MockDiffIntrospector: SchemaIntrospector, @unchecked Sendable {
    let objects: [ObjectIdentifier]

    init(objects: [ObjectIdentifier]) {
        self.objects = objects
    }

    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        var result = objects
        if let schema {
            result = result.filter { $0.schema == schema }
        }
        if let types {
            result = result.filter { types.contains($0.type) }
        }
        return result
    }

    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        TableMetadata(id: id, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
    }
    func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func relationSize(_ id: ObjectIdentifier) async throws -> Int? { nil }
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] { [] }
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] { [] }
    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo { RLSInfo() }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
}

/// Configurable introspector that allows per-object metadata to be set for detailed diff testing.
private final class ConfigurableDiffIntrospector: SchemaIntrospector, @unchecked Sendable {
    let objects: [ObjectIdentifier]
    var tables: [ObjectIdentifier: TableMetadata] = [:]
    var views: [ObjectIdentifier: ViewMetadata] = [:]
    var materializedViews: [ObjectIdentifier: MaterializedViewMetadata] = [:]
    var sequences: [ObjectIdentifier: SequenceMetadata] = [:]
    var enums: [ObjectIdentifier: EnumMetadata] = [:]
    var functions: [ObjectIdentifier: FunctionMetadata] = [:]

    init(objects: [ObjectIdentifier]) {
        self.objects = objects
    }

    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        var result = objects
        if let schema {
            result = result.filter { $0.schema == schema }
        }
        if let types {
            result = result.filter { types.contains($0.type) }
        }
        return result
    }

    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        guard let meta = tables[id] else {
            return TableMetadata(id: id, columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ])
        }
        return meta
    }

    func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata {
        guard let meta = views[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }

    func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata {
        guard let meta = materializedViews[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }

    func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata {
        guard let meta = sequences[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }

    func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata {
        guard let meta = enums[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }

    func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata {
        guard let meta = functions[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }

    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        throw PGSchemaEvoError.objectNotFound(id)
    }
    func relationSize(_ id: ObjectIdentifier) async throws -> Int? { nil }
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] { [] }
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] { [] }
    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo { RLSInfo() }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
}
