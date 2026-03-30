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
}
