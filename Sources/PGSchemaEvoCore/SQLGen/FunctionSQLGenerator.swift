/// Generates DDL for functions and procedures.
///
/// Uses the complete function definition from pg_get_functiondef() which
/// includes the full CREATE FUNCTION/PROCEDURE statement with body.
public struct FunctionSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.function, .procedure] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        guard let func_ = metadata as? FunctionMetadata else {
            throw PGSchemaEvoError.sqlGenerationFailed(
                metadata.id,
                reason: "Expected FunctionMetadata"
            )
        }
        // pg_get_functiondef returns a complete CREATE OR REPLACE statement
        var sql = func_.definition
        if !sql.hasSuffix(";") {
            sql += ";"
        }
        return sql
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        let keyword = id.type == .procedure ? "PROCEDURE" : "FUNCTION"
        let sig = id.signature ?? "()"
        return "DROP \(keyword) IF EXISTS \(id.qualifiedName)\(sig) CASCADE;"
    }
}
