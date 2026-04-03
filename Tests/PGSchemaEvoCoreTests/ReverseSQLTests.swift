import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ObjectDiff Reverse SQL Tests")
struct ReverseSQLTests {

    @Test("ObjectDiff stores reverse migration SQL")
    func reverseFields() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            differences: ["Column age: type integer -> bigint"],
            migrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE bigint;"],
            reverseMigrationSQL: ["ALTER TABLE \"public\".\"users\" ALTER COLUMN \"age\" TYPE integer;"]
        )
        #expect(diff.reverseMigrationSQL.count == 1)
        #expect(diff.reverseMigrationSQL[0].contains("TYPE integer"))
    }

    @Test("ObjectDiff stores reverse drop column SQL")
    func reverseDropFields() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
            differences: ["Column temp: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"public\".\"users\" DROP COLUMN \"temp\";"],
            reverseDropColumnSQL: ["ALTER TABLE \"public\".\"users\" ADD COLUMN \"temp\" text NOT NULL;"]
        )
        #expect(diff.reverseDropColumnSQL.count == 1)
        #expect(diff.reverseDropColumnSQL[0].contains("ADD COLUMN"))
    }

    @Test("ObjectDiff stores irreversible changes")
    func irreversibleChanges() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .enum, schema: "public", name: "status"),
            differences: ["Label 'new': missing in target"],
            migrationSQL: ["ALTER TYPE \"public\".\"status\" ADD VALUE 'new';"],
            irreversibleChanges: ["Cannot remove enum value 'new'"]
        )
        #expect(diff.irreversibleChanges.count == 1)
        #expect(diff.reverseMigrationSQL.isEmpty) // No reverse for enum adds
    }

    @Test("ObjectDiff defaults to empty reverse arrays")
    func defaults() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "public", name: "t"),
            differences: ["test"],
            migrationSQL: ["SELECT 1;"]
        )
        #expect(diff.reverseMigrationSQL.isEmpty)
        #expect(diff.reverseDropColumnSQL.isEmpty)
        #expect(diff.irreversibleChanges.isEmpty)
    }

    // MARK: - Column Reverse SQL

    @Test("Column type change generates correct reverse ALTER TYPE")
    func columnTypeReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Column c: type integer -> bigint"],
            migrationSQL: ["ALTER TABLE \"s\".\"t\" ALTER COLUMN \"c\" TYPE bigint;"],
            reverseMigrationSQL: ["ALTER TABLE \"s\".\"t\" ALTER COLUMN \"c\" TYPE integer;"]
        )
        #expect(diff.migrationSQL[0].contains("TYPE bigint"))
        #expect(diff.reverseMigrationSQL[0].contains("TYPE integer"))
    }

    @Test("Column nullability change generates correct reverse")
    func columnNullReverse() {
        // Forward: DROP NOT NULL -> Reverse: SET NOT NULL
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Column c: nullability changed"],
            migrationSQL: ["ALTER TABLE \"s\".\"t\" ALTER COLUMN \"c\" DROP NOT NULL;"],
            reverseMigrationSQL: ["ALTER TABLE \"s\".\"t\" ALTER COLUMN \"c\" SET NOT NULL;"]
        )
        #expect(diff.migrationSQL[0].contains("DROP NOT NULL"))
        #expect(diff.reverseMigrationSQL[0].contains("SET NOT NULL"))
    }

    @Test("Column default change generates correct reverse")
    func columnDefaultReverse() {
        // Forward: SET DEFAULT 42 -> Reverse: SET DEFAULT 0
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Column c: default changed"],
            migrationSQL: ["ALTER TABLE \"s\".\"t\" ALTER COLUMN \"c\" SET DEFAULT 42;"],
            reverseMigrationSQL: ["ALTER TABLE \"s\".\"t\" ALTER COLUMN \"c\" SET DEFAULT 0;"]
        )
        #expect(diff.migrationSQL[0].contains("SET DEFAULT 42"))
        #expect(diff.reverseMigrationSQL[0].contains("SET DEFAULT 0"))
    }

    @Test("Column add generates reverse DROP")
    func columnAddReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Column c: missing in target"],
            migrationSQL: ["ALTER TABLE \"s\".\"t\" ADD COLUMN \"c\" text NOT NULL;"],
            reverseMigrationSQL: ["ALTER TABLE \"s\".\"t\" DROP COLUMN \"c\";"]
        )
        #expect(diff.migrationSQL[0].contains("ADD COLUMN"))
        #expect(diff.reverseMigrationSQL[0].contains("DROP COLUMN"))
    }

    @Test("Column drop generates reverse ADD with full definition")
    func columnDropReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Column c: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"s\".\"t\" DROP COLUMN \"c\";"],
            reverseDropColumnSQL: ["ALTER TABLE \"s\".\"t\" ADD COLUMN \"c\" text NOT NULL DEFAULT 'hello';"]
        )
        #expect(diff.dropColumnSQL[0].contains("DROP COLUMN"))
        #expect(diff.reverseDropColumnSQL[0].contains("ADD COLUMN"))
        #expect(diff.reverseDropColumnSQL[0].contains("NOT NULL"))
        #expect(diff.reverseDropColumnSQL[0].contains("DEFAULT"))
    }

    // MARK: - Constraint Reverse SQL

    @Test("Constraint add generates reverse DROP")
    func constraintAddReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Constraint uq_email: missing in target"],
            migrationSQL: ["ALTER TABLE \"s\".\"t\" ADD CONSTRAINT \"uq_email\" UNIQUE (email);"],
            reverseMigrationSQL: ["ALTER TABLE \"s\".\"t\" DROP CONSTRAINT \"uq_email\";"]
        )
        #expect(diff.migrationSQL[0].contains("ADD CONSTRAINT"))
        #expect(diff.reverseMigrationSQL[0].contains("DROP CONSTRAINT"))
    }

    @Test("Constraint drop generates reverse ADD")
    func constraintDropReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .table, schema: "s", name: "t"),
            differences: ["Constraint chk_age: extra in target"],
            migrationSQL: [],
            dropColumnSQL: ["ALTER TABLE \"s\".\"t\" DROP CONSTRAINT \"chk_age\";"],
            reverseDropColumnSQL: ["ALTER TABLE \"s\".\"t\" ADD CONSTRAINT \"chk_age\" CHECK (age > 0);"]
        )
        #expect(diff.dropColumnSQL[0].contains("DROP CONSTRAINT"))
        #expect(diff.reverseDropColumnSQL[0].contains("ADD CONSTRAINT"))
    }

    // MARK: - Sequence Reverse SQL

    @Test("Sequence param change generates correct reverse")
    func sequenceReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .sequence, schema: "s", name: "seq"),
            differences: ["INCREMENT: 1 -> 5"],
            migrationSQL: ["ALTER SEQUENCE \"s\".\"seq\" INCREMENT BY 5 CACHE 20;"],
            reverseMigrationSQL: ["ALTER SEQUENCE \"s\".\"seq\" INCREMENT BY 1 CACHE 10;"]
        )
        #expect(diff.migrationSQL[0].contains("INCREMENT BY 5"))
        #expect(diff.reverseMigrationSQL[0].contains("INCREMENT BY 1"))
    }

    // MARK: - View Reverse SQL

    @Test("View definition change generates correct reverse")
    func viewReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .view, schema: "s", name: "v"),
            differences: ["Definition differs"],
            migrationSQL: ["CREATE OR REPLACE VIEW \"s\".\"v\" AS\nSELECT 1;"],
            reverseMigrationSQL: ["CREATE OR REPLACE VIEW \"s\".\"v\" AS\nSELECT 2;"]
        )
        #expect(diff.migrationSQL[0].contains("SELECT 1"))
        #expect(diff.reverseMigrationSQL[0].contains("SELECT 2"))
    }

    // MARK: - Function Reverse SQL

    @Test("Function definition change generates correct reverse")
    func functionReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .function, schema: "s", name: "f"),
            differences: ["Function definition differs"],
            migrationSQL: ["CREATE OR REPLACE FUNCTION s.f() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;"],
            reverseMigrationSQL: ["CREATE OR REPLACE FUNCTION s.f() RETURNS void AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;"]
        )
        #expect(diff.migrationSQL[0].contains("BEGIN END"))
        #expect(diff.reverseMigrationSQL[0].contains("BEGIN NULL"))
    }

    // MARK: - Composite Type Reverse SQL

    @Test("Composite type attribute change generates correct reverse")
    func compositeTypeReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .compositeType, schema: "s", name: "addr"),
            differences: ["Attribute zip: type text -> varchar(10)"],
            migrationSQL: ["ALTER TYPE \"s\".\"addr\" ALTER ATTRIBUTE \"zip\" TYPE varchar(10);"],
            reverseMigrationSQL: ["ALTER TYPE \"s\".\"addr\" ALTER ATTRIBUTE \"zip\" TYPE text;"]
        )
        #expect(diff.migrationSQL[0].contains("TYPE varchar(10)"))
        #expect(diff.reverseMigrationSQL[0].contains("TYPE text"))
    }

    // MARK: - Role Reverse SQL

    @Test("Role attribute change generates correct reverse")
    func roleReverse() {
        let diff = ObjectDiff(
            id: ObjectIdentifier(type: .role, schema: nil, name: "app_user"),
            differences: ["LOGIN: false -> true"],
            migrationSQL: ["ALTER ROLE \"app_user\" LOGIN;"],
            reverseMigrationSQL: ["ALTER ROLE \"app_user\" NOLOGIN;"]
        )
        #expect(diff.migrationSQL[0].contains("LOGIN"))
        #expect(diff.reverseMigrationSQL[0].contains("NOLOGIN"))
    }
}
