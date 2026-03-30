/// Generates DDL for composite types (CREATE TYPE ... AS (...)).
public struct CompositeTypeSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.compositeType] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        guard let meta = metadata as? CompositeTypeMetadata else {
            throw PGSchemaEvoError.sqlGenerationFailed(
                metadata.id,
                reason: "Expected CompositeTypeMetadata"
            )
        }
        return generateCreateComposite(meta)
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        "DROP TYPE IF EXISTS \(id.qualifiedName) CASCADE;"
    }

    private func generateCreateComposite(_ meta: CompositeTypeMetadata) -> String {
        let attributes = meta.attributes.map { attr in
            "    \(quoteIdent(attr.name)) \(attr.dataType)"
        }.joined(separator: ",\n")

        return """
            CREATE TYPE \(meta.id.qualifiedName) AS (
            \(attributes)
            );
            """
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}
