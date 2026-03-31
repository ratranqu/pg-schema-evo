import Testing
@testable import PGSchemaEvoCore

@Suite("TableSQLGenerator Tests")
struct TableSQLGeneratorTests {
    let generator = TableSQLGenerator()

    @Test("Generate simple table with columns")
    func simpleTable() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(name: "name", dataType: "text", isNullable: false, ordinalPosition: 2),
                ColumnInfo(name: "email", dataType: "text", isNullable: true, ordinalPosition: 3),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("CREATE TABLE"))
        #expect(sql.contains("\"public\".\"users\""))
        #expect(sql.contains("\"id\" integer NOT NULL"))
        #expect(sql.contains("\"name\" text NOT NULL"))
        #expect(sql.contains("\"email\" text"))
    }

    @Test("Generate table with default values")
    func tableWithDefaults() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "items"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(
                    name: "created_at",
                    dataType: "timestamp with time zone",
                    isNullable: false,
                    columnDefault: "now()",
                    ordinalPosition: 2
                ),
                ColumnInfo(
                    name: "status",
                    dataType: "text",
                    isNullable: false,
                    columnDefault: "'active'::text",
                    ordinalPosition: 3
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("DEFAULT now()"))
        #expect(sql.contains("DEFAULT 'active'::text"))
    }

    @Test("Generate table with primary key constraint")
    func tableWithPrimaryKey() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
            ],
            constraints: [
                ConstraintInfo(name: "users_pkey", type: .primaryKey, definition: "PRIMARY KEY (id)"),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("CONSTRAINT \"users_pkey\" PRIMARY KEY (id)"))
    }

    @Test("Generate table with foreign key as ALTER TABLE")
    func tableWithForeignKey() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(name: "user_id", dataType: "integer", isNullable: false, ordinalPosition: 2),
            ],
            constraints: [
                ConstraintInfo(
                    name: "orders_user_id_fkey",
                    type: .foreignKey,
                    definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
                    referencedTable: "public.users"
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("ALTER TABLE"))
        #expect(sql.contains("ADD CONSTRAINT \"orders_user_id_fkey\""))
        #expect(sql.contains("FOREIGN KEY (user_id) REFERENCES public.users(id)"))
    }

    @Test("Generate table with identity column")
    func tableWithIdentity() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "items"),
            columns: [
                ColumnInfo(
                    name: "id",
                    dataType: "integer",
                    isNullable: false,
                    ordinalPosition: 1,
                    isIdentity: true,
                    identityGeneration: "ALWAYS"
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("GENERATED ALWAYS AS IDENTITY"))
    }

    @Test("Generate DROP IF EXISTS")
    func dropIfExists() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let sql = generator.generateDrop(for: id)
        #expect(sql == "DROP TABLE IF EXISTS \"public\".\"users\" CASCADE;")
    }

    @Test("Generate table with explicit indexes")
    func tableWithExplicitIndexes() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(name: "customer_id", dataType: "integer", isNullable: false, ordinalPosition: 2),
                ColumnInfo(name: "status", dataType: "text", isNullable: true, ordinalPosition: 3),
            ],
            constraints: [
                ConstraintInfo(name: "orders_pkey", type: .primaryKey, definition: "PRIMARY KEY (id)"),
            ],
            indexes: [
                // Primary key index (should be filtered out)
                IndexInfo(name: "orders_pkey", definition: "CREATE UNIQUE INDEX orders_pkey ON public.orders USING btree (id)", isUnique: true, isPrimary: true),
                // Explicit non-primary, non-constraint index (should be included)
                IndexInfo(name: "idx_orders_customer_id", definition: "CREATE INDEX idx_orders_customer_id ON public.orders USING btree (customer_id)", isUnique: false, isPrimary: false),
                IndexInfo(name: "idx_orders_status", definition: "CREATE INDEX idx_orders_status ON public.orders USING btree (status)", isUnique: false, isPrimary: false),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("CREATE INDEX idx_orders_customer_id ON public.orders USING btree (customer_id);"))
        #expect(sql.contains("CREATE INDEX idx_orders_status ON public.orders USING btree (status);"))
        // Primary key index should NOT appear as a separate CREATE INDEX
        let afterCreateTable = sql.components(separatedBy: ");\n").dropFirst().joined(separator: ");\n")
        #expect(!afterCreateTable.contains("orders_pkey"))
    }

    @Test("Generate table with triggers")
    func tableWithTriggers() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "audit_log"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(name: "data", dataType: "jsonb", isNullable: true, ordinalPosition: 2),
            ],
            triggers: [
                TriggerInfo(
                    name: "trg_audit_insert",
                    definition: "CREATE TRIGGER trg_audit_insert BEFORE INSERT ON public.audit_log FOR EACH ROW EXECUTE FUNCTION public.audit_func()"
                ),
                TriggerInfo(
                    name: "trg_audit_update",
                    definition: "CREATE TRIGGER trg_audit_update BEFORE UPDATE ON public.audit_log FOR EACH ROW EXECUTE FUNCTION public.audit_func()"
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("CREATE TRIGGER trg_audit_insert"))
        #expect(sql.contains("CREATE TRIGGER trg_audit_update"))
        #expect(sql.contains("EXECUTE FUNCTION public.audit_func()"))
    }

    @Test("Identity column with BY DEFAULT generation")
    func identityByDefault() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "things"),
            columns: [
                ColumnInfo(
                    name: "id",
                    dataType: "bigint",
                    isNullable: false,
                    ordinalPosition: 1,
                    isIdentity: true,
                    identityGeneration: "BY DEFAULT"
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        #expect(sql.contains("GENERATED BY DEFAULT AS IDENTITY"))
        // Identity should suppress column default
        #expect(!sql.contains("DEFAULT"))
    }

    @Test("Identity column without identityGeneration does not emit GENERATED clause")
    func identityWithoutGeneration() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "things"),
            columns: [
                ColumnInfo(
                    name: "id",
                    dataType: "integer",
                    isNullable: false,
                    columnDefault: "nextval('things_id_seq')",
                    ordinalPosition: 1,
                    isIdentity: true,
                    identityGeneration: nil
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        // isIdentity is true but identityGeneration is nil, so it falls through to the default branch
        #expect(!sql.contains("GENERATED"))
        #expect(sql.contains("DEFAULT nextval('things_id_seq')"))
    }

    @Test("Wrong metadata type throws error")
    func wrongMetadataThrows() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let metadata = EnumMetadata(id: id, labels: ["a", "b"])
        #expect(throws: PGSchemaEvoError.self) {
            try generator.generateCreate(from: metadata)
        }
    }

    @Test("Table with unique constraint index is filtered from explicit indexes")
    func uniqueConstraintIndexFiltered() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(name: "email", dataType: "text", isNullable: false, ordinalPosition: 2),
            ],
            constraints: [
                ConstraintInfo(name: "users_pkey", type: .primaryKey, definition: "PRIMARY KEY (id)"),
                ConstraintInfo(name: "users_email_key", type: .unique, definition: "UNIQUE (email)"),
            ],
            indexes: [
                IndexInfo(name: "users_pkey", definition: "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)", isUnique: true, isPrimary: true),
                // This index has the same name as the unique constraint, so it should be filtered
                IndexInfo(name: "users_email_key", definition: "CREATE UNIQUE INDEX users_email_key ON public.users USING btree (email)", isUnique: true, isPrimary: false),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        // The inline constraint definition should be present
        #expect(sql.contains("CONSTRAINT \"users_email_key\" UNIQUE (email)"))
        // But the separate CREATE INDEX for the constraint-backed index should NOT appear
        let parts = sql.components(separatedBy: ");\n")
        let afterTable = parts.count > 1 ? parts.dropFirst().joined(separator: ");\n") : ""
        #expect(!afterTable.contains("CREATE UNIQUE INDEX users_email_key"))
    }

    @Test("Table with FK and other constraints separates them correctly")
    func fkSeparatedFromInlineConstraints() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "orders"),
            columns: [
                ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1),
                ColumnInfo(name: "user_id", dataType: "integer", isNullable: false, ordinalPosition: 2),
                ColumnInfo(name: "amount", dataType: "numeric", isNullable: false, ordinalPosition: 3),
            ],
            constraints: [
                ConstraintInfo(name: "orders_pkey", type: .primaryKey, definition: "PRIMARY KEY (id)"),
                ConstraintInfo(name: "orders_amount_check", type: .check, definition: "CHECK (amount > 0)"),
                ConstraintInfo(
                    name: "orders_user_fk",
                    type: .foreignKey,
                    definition: "FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE",
                    referencedTable: "public.users"
                ),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        // PK and CHECK should be inline
        #expect(sql.contains("CONSTRAINT \"orders_pkey\" PRIMARY KEY (id)"))
        #expect(sql.contains("CONSTRAINT \"orders_amount_check\" CHECK (amount > 0)"))
        // FK should be separate ALTER TABLE
        #expect(sql.contains("ALTER TABLE \"public\".\"orders\""))
        #expect(sql.contains("ADD CONSTRAINT \"orders_user_fk\" FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE"))
    }

    @Test("Column ordering is respected")
    func columnOrdering() throws {
        let metadata = TableMetadata(
            id: ObjectIdentifier(type: .table, schema: "public", name: "test"),
            columns: [
                ColumnInfo(name: "c", dataType: "text", isNullable: true, ordinalPosition: 3),
                ColumnInfo(name: "a", dataType: "text", isNullable: true, ordinalPosition: 1),
                ColumnInfo(name: "b", dataType: "text", isNullable: true, ordinalPosition: 2),
            ]
        )

        let sql = try generator.generateCreate(from: metadata)
        let aRange = sql.range(of: "\"a\" text")!
        let bRange = sql.range(of: "\"b\" text")!
        let cRange = sql.range(of: "\"c\" text")!
        #expect(aRange.lowerBound < bRange.lowerBound)
        #expect(bRange.lowerBound < cRange.lowerBound)
    }
}
