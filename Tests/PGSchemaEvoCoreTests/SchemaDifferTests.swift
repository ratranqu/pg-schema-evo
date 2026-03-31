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

    @Test("compareObject returns nil for unsupported types like aggregate")
    func unsupportedTypeReturnsNil() async throws {
        let aggId = ObjectIdentifier(type: .aggregate, schema: "public", name: "my_agg")
        let source = ConfigurableDiffIntrospector(objects: [aggId])
        let target = ConfigurableDiffIntrospector(objects: [aggId])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        // Aggregate type goes through default branch which returns nil, so no modifications
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

    // MARK: - Composite Type Diffs

    @Test("Composite type diff detects attribute type change")
    func compositeTypeAttrTypeChange() async throws {
        let typeId = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let source = ConfigurableDiffIntrospector(objects: [typeId])
        source.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
            CompositeTypeAttribute(name: "zip", dataType: "varchar(10)", ordinalPosition: 2),
        ])
        let target = ConfigurableDiffIntrospector(objects: [typeId])
        target.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
            CompositeTypeAttribute(name: "zip", dataType: "varchar(5)", ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences[0].contains("zip"))
        #expect(result.modified[0].migrationSQL[0].contains("ALTER TYPE"))
    }

    @Test("Composite type diff detects missing attribute")
    func compositeTypeMissingAttr() async throws {
        let typeId = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let source = ConfigurableDiffIntrospector(objects: [typeId])
        source.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
            CompositeTypeAttribute(name: "city", dataType: "text", ordinalPosition: 2),
        ])
        let target = ConfigurableDiffIntrospector(objects: [typeId])
        target.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences[0].contains("city"))
        #expect(result.modified[0].migrationSQL[0].contains("ADD ATTRIBUTE"))
    }

    @Test("Composite type diff detects extra attribute in target")
    func compositeTypeExtraAttr() async throws {
        let typeId = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let source = ConfigurableDiffIntrospector(objects: [typeId])
        source.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [typeId])
        target.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
            CompositeTypeAttribute(name: "obsolete", dataType: "text", ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].migrationSQL[0].contains("DROP ATTRIBUTE"))
    }

    @Test("Composite type identical produces no diff")
    func compositeTypeIdentical() async throws {
        let typeId = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        let source = ConfigurableDiffIntrospector(objects: [typeId])
        source.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [typeId])
        target.compositeTypes[typeId] = CompositeTypeMetadata(id: typeId, attributes: [
            CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    // MARK: - Schema Diffs

    @Test("Schema diff detects owner change")
    func schemaOwnerChange() async throws {
        let schemaId = ObjectIdentifier(type: .schema, schema: nil, name: "myschema")
        let source = ConfigurableDiffIntrospector(objects: [schemaId])
        source.schemas[schemaId] = SchemaMetadata(id: schemaId, owner: "admin")
        let target = ConfigurableDiffIntrospector(objects: [schemaId])
        target.schemas[schemaId] = SchemaMetadata(id: schemaId, owner: "postgres")

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].migrationSQL[0].contains("ALTER SCHEMA"))
        #expect(result.modified[0].migrationSQL[0].contains("OWNER TO"))
    }

    @Test("Schema identical produces no diff")
    func schemaIdentical() async throws {
        let schemaId = ObjectIdentifier(type: .schema, schema: nil, name: "myschema")
        let source = ConfigurableDiffIntrospector(objects: [schemaId])
        source.schemas[schemaId] = SchemaMetadata(id: schemaId, owner: "postgres")
        let target = ConfigurableDiffIntrospector(objects: [schemaId])
        target.schemas[schemaId] = SchemaMetadata(id: schemaId, owner: "postgres")

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    // MARK: - Role Diffs

    @Test("Role diff detects attribute changes")
    func roleAttrChanges() async throws {
        let roleId = ObjectIdentifier(type: .role, schema: nil, name: "app_user")
        let source = ConfigurableDiffIntrospector(objects: [roleId])
        source.roles[roleId] = RoleMetadata(id: roleId, canLogin: true, canCreateDB: true)
        let target = ConfigurableDiffIntrospector(objects: [roleId])
        target.roles[roleId] = RoleMetadata(id: roleId, canLogin: false, canCreateDB: false)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.count == 2)
        let sql = result.modified[0].migrationSQL[0]
        #expect(sql.contains("ALTER ROLE"))
        #expect(sql.contains("LOGIN"))
        #expect(sql.contains("CREATEDB"))
    }

    @Test("Role diff detects membership changes")
    func roleMembershipChanges() async throws {
        let roleId = ObjectIdentifier(type: .role, schema: nil, name: "app_user")
        let source = ConfigurableDiffIntrospector(objects: [roleId])
        source.roles[roleId] = RoleMetadata(id: roleId, memberOf: ["readers", "writers"])
        let target = ConfigurableDiffIntrospector(objects: [roleId])
        target.roles[roleId] = RoleMetadata(id: roleId, memberOf: ["readers", "admins"])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        let sqlJoined = result.modified[0].migrationSQL.joined(separator: " ")
        #expect(sqlJoined.contains("GRANT"))
        #expect(sqlJoined.contains("REVOKE"))
    }

    @Test("Role identical produces no diff")
    func roleIdentical() async throws {
        let roleId = ObjectIdentifier(type: .role, schema: nil, name: "app_user")
        let source = ConfigurableDiffIntrospector(objects: [roleId])
        source.roles[roleId] = RoleMetadata(id: roleId, canLogin: true, memberOf: ["readers"])
        let target = ConfigurableDiffIntrospector(objects: [roleId])
        target.roles[roleId] = RoleMetadata(id: roleId, canLogin: true, memberOf: ["readers"])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    // MARK: - Extension Diffs

    @Test("Extension diff detects version change")
    func extensionVersionChange() async throws {
        let extId = ObjectIdentifier(type: .extension, schema: nil, name: "uuid-ossp")
        let source = ConfigurableDiffIntrospector(objects: [extId])
        source.extensions[extId] = ExtensionMetadata(id: extId, version: "1.2")
        let target = ConfigurableDiffIntrospector(objects: [extId])
        target.extensions[extId] = ExtensionMetadata(id: extId, version: "1.1")

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].migrationSQL[0].contains("ALTER EXTENSION"))
        #expect(result.modified[0].migrationSQL[0].contains("UPDATE TO"))
    }

    @Test("Extension identical produces no diff")
    func extensionIdentical() async throws {
        let extId = ObjectIdentifier(type: .extension, schema: nil, name: "uuid-ossp")
        let source = ConfigurableDiffIntrospector(objects: [extId])
        source.extensions[extId] = ExtensionMetadata(id: extId, version: "1.1")
        let target = ConfigurableDiffIntrospector(objects: [extId])
        target.extensions[extId] = ExtensionMetadata(id: extId, version: "1.1")

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.isEmpty)
    }

    // MARK: - DROP column/constraint/index SQL

    @Test("Extra column in target generates DROP COLUMN in dropColumnSQL")
    func dropColumnSQL() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "legacy", dataType: "text", isNullable: true, ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("DROP COLUMN") && $0.contains("legacy") })
        #expect(result.modified[0].migrationSQL.allSatisfy { !$0.contains("DROP COLUMN") })
    }

    @Test("Extra constraint in target generates DROP CONSTRAINT in dropColumnSQL")
    func dropConstraintSQL() async throws {
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
                ConstraintInfo(name: "orders_old_check", type: .check, definition: "CHECK (amount > 0)"),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("DROP CONSTRAINT") && $0.contains("orders_old_check") })
    }

    @Test("Extra index in target generates DROP INDEX in dropColumnSQL")
    func dropIndexSQL() async throws {
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
                IndexInfo(name: "idx_old", definition: "CREATE INDEX idx_old ON public.orders (old_col)", isUnique: false, isPrimary: false),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("DROP INDEX") && $0.contains("idx_old") })
    }

    // MARK: - Trigger comparison

    @Test("Missing trigger in target generates CREATE TRIGGER")
    func triggerMissingInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            triggers: [
                TriggerInfo(name: "trg_audit", definition: "CREATE TRIGGER trg_audit AFTER INSERT ON public.orders FOR EACH ROW EXECUTE FUNCTION audit_log()"),
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
        #expect(result.modified[0].differences.contains { $0.contains("trg_audit") && $0.contains("missing in target") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("CREATE TRIGGER trg_audit") })
    }

    @Test("Extra trigger in target generates DROP TRIGGER in dropColumnSQL")
    func triggerExtraInTarget() async throws {
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
            triggers: [
                TriggerInfo(name: "trg_old", definition: "CREATE TRIGGER trg_old AFTER DELETE ON public.orders FOR EACH ROW EXECUTE FUNCTION cleanup()"),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("trg_old") && $0.contains("extra in target") })
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("DROP TRIGGER") && $0.contains("trg_old") })
    }

    @Test("Trigger definition difference generates DROP+CREATE")
    func triggerDefinitionDiffers() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            triggers: [
                TriggerInfo(name: "trg_notify", definition: "CREATE TRIGGER trg_notify AFTER INSERT ON public.orders FOR EACH ROW EXECUTE FUNCTION notify_v2()"),
            ]
        )
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)],
            triggers: [
                TriggerInfo(name: "trg_notify", definition: "CREATE TRIGGER trg_notify AFTER INSERT ON public.orders FOR EACH ROW EXECUTE FUNCTION notify_v1()"),
            ]
        )

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("trg_notify") && $0.contains("definition differs") })
        let sql = result.modified[0].migrationSQL.joined(separator: "\n")
        #expect(sql.contains("DROP TRIGGER"))
        #expect(sql.contains("notify_v2"))
    }

    // MARK: - RLS policy comparison

    @Test("RLS enabled on source but not target generates ENABLE RLS")
    func rlsEnableOnTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: true)
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: false)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("RLS") && $0.contains("not enabled") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("ENABLE ROW LEVEL SECURITY") })
    }

    @Test("RLS enabled on target but not source generates DISABLE RLS in dropColumnSQL")
    func rlsDisableOnTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: false)
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: true)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("DISABLE ROW LEVEL SECURITY") })
    }

    @Test("RLS forced on source but not target generates FORCE RLS")
    func rlsForceOnTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: true, isForced: true)
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: true, isForced: false)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].migrationSQL.contains { $0.contains("FORCE ROW LEVEL SECURITY") })
    }

    @Test("RLS policy missing in target generates CREATE POLICY")
    func rlsPolicyMissingInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: true, policies: [
            RLSPolicy(name: "user_access", definition: "CREATE POLICY user_access ON public.secrets FOR SELECT USING (user_id = current_user_id())"),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: true)

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("user_access") && $0.contains("missing in target") })
        #expect(result.modified[0].migrationSQL.contains { $0.contains("CREATE POLICY user_access") })
    }

    @Test("RLS policy extra in target generates DROP POLICY in dropColumnSQL")
    func rlsPolicyExtraInTarget() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: true)
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: true, policies: [
            RLSPolicy(name: "old_policy", definition: "CREATE POLICY old_policy ON public.secrets FOR ALL USING (true)"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].dropColumnSQL.contains { $0.contains("DROP POLICY") && $0.contains("old_policy") })
    }

    @Test("RLS policy definition differs generates DROP+CREATE")
    func rlsPolicyDefinitionDiffers() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "secrets")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        source.rlsInfos[tableId] = RLSInfo(isEnabled: true, policies: [
            RLSPolicy(name: "access", definition: "CREATE POLICY access ON public.secrets USING (role = 'admin')"),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(
            id: tableId,
            columns: [ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)]
        )
        target.rlsInfos[tableId] = RLSInfo(isEnabled: true, policies: [
            RLSPolicy(name: "access", definition: "CREATE POLICY access ON public.secrets USING (role = 'user')"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        #expect(result.modified.count == 1)
        #expect(result.modified[0].differences.contains { $0.contains("access") && $0.contains("definition differs") })
        let sql = result.modified[0].migrationSQL.joined(separator: "\n")
        #expect(sql.contains("DROP POLICY"))
        #expect(sql.contains("role = 'admin'"))
    }

    // MARK: - renderMigrationSQL enhancements

    @Test("renderMigrationSQL shows destructive changes as comments by default")
    func renderMigrationSQLDestructiveSkipped() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "old_col", dataType: "text", isNullable: true, ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sql = result.renderMigrationSQL()
        #expect(sql.contains("-- Destructive changes SKIPPED"))
        #expect(sql.contains("-- ALTER TABLE"))
        #expect(sql.contains("DROP COLUMN"))
    }

    @Test("renderMigrationSQL includes destructive changes when flag is set")
    func renderMigrationSQLDestructiveIncluded() async throws {
        let tableId = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let source = ConfigurableDiffIntrospector(objects: [tableId])
        source.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
        ])
        let target = ConfigurableDiffIntrospector(objects: [tableId])
        target.tables[tableId] = TableMetadata(id: tableId, columns: [
            ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ColumnInfo(name: "old_col", dataType: "text", isNullable: true, ordinalPosition: 2),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sql = result.renderMigrationSQL(includeDestructive: true)
        #expect(sql.contains("ALTER TABLE"))
        #expect(sql.contains("DROP COLUMN"))
        #expect(!sql.contains("SKIPPED"))
    }

    @Test("renderMigrationSQL includes DROP for objects only in target when destructive")
    func renderMigrationSQLDropOnlyInTarget() async throws {
        let source = ConfigurableDiffIntrospector(objects: [])
        let target = ConfigurableDiffIntrospector(objects: [
            ObjectIdentifier(type: .table, schema: "public", name: "orphan"),
        ])

        let differ = SchemaDiffer(logger: Logger(label: "test"))
        let result = try await differ.diff(source: source, target: target)

        let sqlDefault = result.renderMigrationSQL()
        #expect(sqlDefault.contains("-- DROP TABLE IF EXISTS"))

        let sqlDestructive = result.renderMigrationSQL(includeDestructive: true)
        #expect(sqlDestructive.contains("DROP TABLE IF EXISTS"))
        #expect(!sqlDestructive.contains("-- DROP TABLE IF EXISTS"))
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
    func primaryKeyColumns(for id: ObjectIdentifier) async throws -> [String] { [] }
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

    var schemas: [ObjectIdentifier: SchemaMetadata] = [:]
    var roles: [ObjectIdentifier: RoleMetadata] = [:]
    var compositeTypes: [ObjectIdentifier: CompositeTypeMetadata] = [:]
    var extensions: [ObjectIdentifier: ExtensionMetadata] = [:]
    var rlsInfos: [ObjectIdentifier: RLSInfo] = [:]

    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo {
        rlsInfos[id] ?? RLSInfo()
    }

    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        guard let meta = schemas[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }
    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        guard let meta = roles[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }
    func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        guard let meta = compositeTypes[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }
    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        guard let meta = extensions[id] else {
            throw PGSchemaEvoError.objectNotFound(id)
        }
        return meta
    }
    func relationSize(_ id: ObjectIdentifier) async throws -> Int? { nil }
    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] { [] }
    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] { [] }
    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? { nil }
    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] { [] }
    func primaryKeyColumns(for id: ObjectIdentifier) async throws -> [String] { [] }
}
