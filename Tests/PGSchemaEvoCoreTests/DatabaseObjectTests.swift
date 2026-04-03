import Testing
@testable import PGSchemaEvoCore

@Suite("DatabaseObject Tests")
struct DatabaseObjectTests {

    // MARK: - ObjectIdentifier

    @Test("ObjectIdentifier description includes type and qualified name")
    func identifierDescription() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        #expect(id.description == "table:public.users")
    }

    @Test("ObjectIdentifier description for schema-less objects")
    func identifierNoSchema() {
        let id = ObjectIdentifier(type: .role, name: "admin")
        #expect(id.description == "role:admin")
    }

    @Test("ObjectIdentifier description with function signature")
    func identifierWithSignature() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "calculate", signature: "(integer)")
        #expect(id.description == "function:public.calculate(integer)")
    }

    @Test("ObjectIdentifier qualifiedName quotes identifiers")
    func qualifiedNameQuoting() {
        let id = ObjectIdentifier(type: .table, schema: "my schema", name: "my table")
        #expect(id.qualifiedName == "\"my schema\".\"my table\"")
    }

    @Test("ObjectIdentifier equality")
    func identifierEquality() {
        let a = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let b = ObjectIdentifier(type: .table, schema: "public", name: "users")
        #expect(a == b)
    }

    @Test("ObjectIdentifier inequality by type")
    func identifierInequalityByType() {
        let a = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let b = ObjectIdentifier(type: .view, schema: "public", name: "users")
        #expect(a != b)
    }

    // MARK: - ObjectType properties

    @Test("Table supports data")
    func tableSupportsData() {
        #expect(ObjectType.table.supportsData)
    }

    @Test("View does not support data")
    func viewNoData() {
        #expect(!ObjectType.view.supportsData)
    }

    @Test("Role is not schema-scoped")
    func roleNotSchemaScoped() {
        #expect(!ObjectType.role.isSchemaScoped)
    }

    @Test("Table is schema-scoped")
    func tableSchemaScoped() {
        #expect(ObjectType.table.isSchemaScoped)
    }

    // MARK: - parseObjectSpecifier

    @Test("Parse table specifier with schema")
    func parseTableWithSchema() throws {
        let id = try parseObjectSpecifier("table:myschema.orders")
        #expect(id.type == .table)
        #expect(id.schema == "myschema")
        #expect(id.name == "orders")
    }

    @Test("Parse table specifier without schema defaults to public")
    func parseTableDefaultSchema() throws {
        let id = try parseObjectSpecifier("table:orders")
        #expect(id.type == .table)
        #expect(id.schema == "public")
        #expect(id.name == "orders")
    }

    @Test("Parse function specifier with signature")
    func parseFunctionWithSignature() throws {
        let id = try parseObjectSpecifier("function:public.calculate_total(integer)")
        #expect(id.type == .function)
        #expect(id.schema == "public")
        #expect(id.name == "calculate_total")
        #expect(id.signature == "(integer)")
    }

    @Test("Parse role specifier (no schema)")
    func parseRoleNoSchema() throws {
        let id = try parseObjectSpecifier("role:admin")
        #expect(id.type == .role)
        #expect(id.schema == nil)
        #expect(id.name == "admin")
    }

    @Test("Parse materialized view")
    func parseMatview() throws {
        let id = try parseObjectSpecifier("matview:analytics.daily_stats")
        #expect(id.type == .materializedView)
        #expect(id.schema == "analytics")
        #expect(id.name == "daily_stats")
    }

    @Test("Reject unknown object type")
    func rejectUnknownType() {
        #expect(throws: PGSchemaEvoError.self) {
            try parseObjectSpecifier("bogus:public.foo")
        }
    }

    @Test("Reject missing colon")
    func rejectMissingColon() {
        #expect(throws: PGSchemaEvoError.self) {
            try parseObjectSpecifier("table.public.foo")
        }
    }

    @Test("Reject empty object name")
    func rejectEmptyName() {
        #expect(throws: PGSchemaEvoError.self) {
            try parseObjectSpecifier("table:")
        }
    }

    // MARK: - ObjectType.displayName (all types)

    @Test("displayName for materializedView")
    func displayNameMatview() {
        #expect(ObjectType.materializedView.displayName == "materialized view")
    }

    @Test("displayName for compositeType")
    func displayNameCompositeType() {
        #expect(ObjectType.compositeType.displayName == "composite type")
    }

    @Test("displayName for aggregate")
    func displayNameAggregate() {
        #expect(ObjectType.aggregate.displayName == "aggregate")
    }

    @Test("displayName for operator")
    func displayNameOperator() {
        #expect(ObjectType.operator.displayName == "operator")
    }

    @Test("displayName for foreignDataWrapper")
    func displayNameFDW() {
        #expect(ObjectType.foreignDataWrapper.displayName == "foreign data wrapper")
    }

    @Test("displayName for foreignTable")
    func displayNameForeignTable() {
        #expect(ObjectType.foreignTable.displayName == "foreign table")
    }

    @Test("displayName for sequence")
    func displayNameSequence() {
        #expect(ObjectType.sequence.displayName == "sequence")
    }

    @Test("displayName for enum")
    func displayNameEnum() {
        #expect(ObjectType.enum.displayName == "enum")
    }

    @Test("displayName for procedure")
    func displayNameProcedure() {
        #expect(ObjectType.procedure.displayName == "procedure")
    }

    @Test("displayName for schema")
    func displayNameSchema() {
        #expect(ObjectType.schema.displayName == "schema")
    }

    @Test("displayName for extension")
    func displayNameExtension() {
        #expect(ObjectType.extension.displayName == "extension")
    }

    @Test("displayName for function")
    func displayNameFunction() {
        #expect(ObjectType.function.displayName == "function")
    }

    @Test("displayName for role")
    func displayNameRole() {
        #expect(ObjectType.role.displayName == "role")
    }

    @Test("displayName for table")
    func displayNameTable() {
        #expect(ObjectType.table.displayName == "table")
    }

    @Test("displayName for view")
    func displayNameView() {
        #expect(ObjectType.view.displayName == "view")
    }

    // MARK: - ObjectType.supportsData (additional cases)

    @Test("materializedView supports data")
    func matviewSupportsData() {
        #expect(ObjectType.materializedView.supportsData == true)
    }

    @Test("foreignTable supports data")
    func foreignTableSupportsData() {
        #expect(ObjectType.foreignTable.supportsData == true)
    }

    @Test("enum does not support data")
    func enumNoData() {
        #expect(ObjectType.enum.supportsData == false)
    }

    @Test("function does not support data")
    func functionNoData() {
        #expect(ObjectType.function.supportsData == false)
    }

    @Test("sequence does not support data")
    func sequenceNoData() {
        #expect(ObjectType.sequence.supportsData == false)
    }

    @Test("compositeType does not support data")
    func compositeTypeNoData() {
        #expect(ObjectType.compositeType.supportsData == false)
    }

    @Test("procedure does not support data")
    func procedureNoData() {
        #expect(ObjectType.procedure.supportsData == false)
    }

    // MARK: - ObjectType.isSchemaScoped (additional cases)

    @Test("extension is not schema-scoped")
    func extensionNotSchemaScoped() {
        #expect(ObjectType.extension.isSchemaScoped == false)
    }

    @Test("schema is not schema-scoped")
    func schemaNotSchemaScoped() {
        #expect(ObjectType.schema.isSchemaScoped == false)
    }

    @Test("function is schema-scoped")
    func functionSchemaScoped() {
        #expect(ObjectType.function.isSchemaScoped == true)
    }

    @Test("view is schema-scoped")
    func viewSchemaScoped() {
        #expect(ObjectType.view.isSchemaScoped == true)
    }

    @Test("enum is schema-scoped")
    func enumSchemaScoped() {
        #expect(ObjectType.enum.isSchemaScoped == true)
    }

    @Test("foreignTable is schema-scoped")
    func foreignTableSchemaScoped() {
        #expect(ObjectType.foreignTable.isSchemaScoped == true)
    }

    @Test("foreignDataWrapper is schema-scoped")
    func fdwSchemaScoped() {
        #expect(ObjectType.foreignDataWrapper.isSchemaScoped == true)
    }

    // MARK: - ObjectIdentifier qualifiedName without schema

    @Test("qualifiedName without schema returns just quoted name")
    func qualifiedNameNoSchema() {
        let id = ObjectIdentifier(type: .role, name: "admin")
        #expect(id.qualifiedName == "\"admin\"")
    }

    @Test("qualifiedName quotes embedded double quotes")
    func qualifiedNameEmbeddedQuotes() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "my\"table")
        #expect(id.qualifiedName == "\"public\".\"my\"\"table\"")
    }

    // MARK: - ObjectIdentifier description edge cases

    @Test("description with signature but no schema")
    func descriptionSignatureNoSchema() {
        let id = ObjectIdentifier(type: .function, name: "myfunc", signature: "(text, integer)")
        #expect(id.description == "function:myfunc(text, integer)")
    }

    @Test("description for materializedView uses raw value")
    func descriptionMatview() {
        let id = ObjectIdentifier(type: .materializedView, schema: "analytics", name: "stats")
        #expect(id.description == "matview:analytics.stats")
    }

    @Test("description for foreignDataWrapper uses raw value")
    func descriptionFDW() {
        let id = ObjectIdentifier(type: .foreignDataWrapper, name: "postgres_fdw")
        #expect(id.description == "fdw:postgres_fdw")
    }

    @Test("description for foreignTable uses raw value")
    func descriptionForeignTable() {
        let id = ObjectIdentifier(type: .foreignTable, schema: "public", name: "remote_users")
        #expect(id.description == "foreign_table:public.remote_users")
    }

    @Test("description for compositeType uses raw value")
    func descriptionCompositeType() {
        let id = ObjectIdentifier(type: .compositeType, schema: "public", name: "address")
        #expect(id.description == "type:public.address")
    }

    // MARK: - parseObjectSpecifier for additional object types

    @Test("Parse enum specifier")
    func parseEnum() throws {
        let id = try parseObjectSpecifier("enum:public.status_type")
        #expect(id.type == .enum)
        #expect(id.schema == "public")
        #expect(id.name == "status_type")
    }

    @Test("Parse composite type specifier")
    func parseCompositeType() throws {
        let id = try parseObjectSpecifier("type:public.address")
        #expect(id.type == .compositeType)
        #expect(id.schema == "public")
        #expect(id.name == "address")
    }

    @Test("Parse sequence specifier")
    func parseSequence() throws {
        let id = try parseObjectSpecifier("sequence:public.users_id_seq")
        #expect(id.type == .sequence)
        #expect(id.schema == "public")
        #expect(id.name == "users_id_seq")
    }

    @Test("Parse procedure specifier")
    func parseProcedure() throws {
        let id = try parseObjectSpecifier("procedure:public.do_cleanup(integer)")
        #expect(id.type == .procedure)
        #expect(id.schema == "public")
        #expect(id.name == "do_cleanup")
        #expect(id.signature == "(integer)")
    }

    @Test("Parse aggregate specifier")
    func parseAggregate() throws {
        let id = try parseObjectSpecifier("aggregate:public.my_agg")
        #expect(id.type == .aggregate)
        #expect(id.schema == "public")
        #expect(id.name == "my_agg")
    }

    @Test("Parse operator specifier")
    func parseOperator() throws {
        let id = try parseObjectSpecifier("operator:public.&&")
        #expect(id.type == .operator)
        #expect(id.schema == "public")
        #expect(id.name == "&&")
    }

    @Test("Parse fdw specifier without schema")
    func parseFDW() throws {
        let id = try parseObjectSpecifier("fdw:postgres_fdw")
        #expect(id.type == .foreignDataWrapper)
        #expect(id.schema == "public")
        #expect(id.name == "postgres_fdw")
    }

    @Test("Parse foreign_table specifier")
    func parseForeignTable() throws {
        let id = try parseObjectSpecifier("foreign_table:public.remote_users")
        #expect(id.type == .foreignTable)
        #expect(id.schema == "public")
        #expect(id.name == "remote_users")
    }

    @Test("Parse extension specifier has no schema")
    func parseExtension() throws {
        let id = try parseObjectSpecifier("extension:postgis")
        #expect(id.type == .extension)
        #expect(id.schema == nil)
        #expect(id.name == "postgis")
    }

    @Test("Parse schema specifier has no schema")
    func parseSchema() throws {
        let id = try parseObjectSpecifier("schema:analytics")
        #expect(id.type == .schema)
        #expect(id.schema == nil)
        #expect(id.name == "analytics")
    }

    @Test("Parse view specifier without schema defaults to public")
    func parseViewDefaultSchema() throws {
        let id = try parseObjectSpecifier("view:active_users")
        #expect(id.type == .view)
        #expect(id.schema == "public")
        #expect(id.name == "active_users")
    }

    // MARK: - ObjectSpec initializer defaults

    @Test("ObjectSpec initializer uses correct defaults")
    func objectSpecDefaults() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let spec = ObjectSpec(id: id)
        #expect(spec.copyPermissions == false)
        #expect(spec.copyData == false)
        #expect(spec.cascadeDependencies == false)
        #expect(spec.whereClause == nil)
        #expect(spec.rowLimit == nil)
        #expect(spec.copyRLSPolicies == false)
    }

    @Test("ObjectSpec initializer preserves all custom values")
    func objectSpecCustomValues() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let spec = ObjectSpec(
            id: id,
            copyPermissions: true,
            copyData: true,
            cascadeDependencies: true,
            whereClause: "active = true",
            rowLimit: 1000,
            copyRLSPolicies: true
        )
        #expect(spec.copyPermissions == true)
        #expect(spec.copyData == true)
        #expect(spec.cascadeDependencies == true)
        #expect(spec.whereClause == "active = true")
        #expect(spec.rowLimit == 1000)
        #expect(spec.copyRLSPolicies == true)
    }

    // MARK: - ObjectIdentifier hashability

    @Test("ObjectIdentifier can be used as dictionary key")
    func identifierHashable() {
        let id1 = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let id2 = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        var dict: [ObjectIdentifier: String] = [:]
        dict[id1] = "first"
        dict[id2] = "second"
        #expect(dict[id1] == "first")
        #expect(dict[id2] == "second")
        #expect(dict.count == 2)
    }
}
