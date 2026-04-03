import Testing
@testable import PGSchemaEvoCore

@Suite("SQLGenerator supportedTypes Tests")
struct SQLGeneratorSupportedTypesTests {

    @Test("TableSQLGenerator supports table type")
    func tableSupportedTypes() {
        let gen = TableSQLGenerator()
        #expect(gen.supportedTypes == [.table])
    }

    @Test("ViewSQLGenerator supports view and materializedView types")
    func viewSupportedTypes() {
        let gen = ViewSQLGenerator()
        #expect(gen.supportedTypes == [.view, .materializedView])
    }

    @Test("SequenceSQLGenerator supports sequence type")
    func sequenceSupportedTypes() {
        let gen = SequenceSQLGenerator()
        #expect(gen.supportedTypes == [.sequence])
    }

    @Test("EnumSQLGenerator supports enum type")
    func enumSupportedTypes() {
        let gen = EnumSQLGenerator()
        #expect(gen.supportedTypes == [.enum])
    }

    @Test("FunctionSQLGenerator supports function and procedure types")
    func functionSupportedTypes() {
        let gen = FunctionSQLGenerator()
        #expect(gen.supportedTypes == [.function, .procedure])
    }

    @Test("SchemaSQLGenerator supports schema, role, and extension types")
    func schemaSupportedTypes() {
        let gen = SchemaSQLGenerator()
        #expect(gen.supportedTypes == [.schema, .role, .extension])
    }

    @Test("CompositeTypeSQLGenerator supports compositeType")
    func compositeTypeSupportedTypes() {
        let gen = CompositeTypeSQLGenerator()
        #expect(gen.supportedTypes == [.compositeType])
    }
}
