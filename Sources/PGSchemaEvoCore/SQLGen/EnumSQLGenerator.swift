/// Generates DDL for enum types.
public struct EnumSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.enum] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        guard let enumMeta = metadata as? EnumMetadata else {
            throw PGSchemaEvoError.sqlGenerationFailed(
                metadata.id,
                reason: "Expected EnumMetadata"
            )
        }
        return generateCreateEnum(enumMeta)
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        "DROP TYPE IF EXISTS \(id.qualifiedName) CASCADE;"
    }

    private func generateCreateEnum(_ enumMeta: EnumMetadata) -> String {
        let labels = enumMeta.labels.map { "'\(escapeSQLString($0))'" }.joined(separator: ",\n    ")
        return """
            CREATE TYPE \(enumMeta.id.qualifiedName) AS ENUM (
                \(labels)
            );
            """
    }

    private func escapeSQLString(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}
