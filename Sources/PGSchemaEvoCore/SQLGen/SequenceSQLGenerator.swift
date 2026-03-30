/// Generates DDL for sequences.
public struct SequenceSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.sequence] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        guard let seq = metadata as? SequenceMetadata else {
            throw PGSchemaEvoError.sqlGenerationFailed(
                metadata.id,
                reason: "Expected SequenceMetadata"
            )
        }
        return generateCreateSequence(seq)
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        "DROP SEQUENCE IF EXISTS \(id.qualifiedName) CASCADE;"
    }

    private func generateCreateSequence(_ seq: SequenceMetadata) -> String {
        var sql = "CREATE SEQUENCE \(seq.id.qualifiedName)"
        sql += "\n    AS \(seq.dataType)"
        sql += "\n    START WITH \(seq.startValue)"
        sql += "\n    INCREMENT BY \(seq.increment)"
        sql += "\n    MINVALUE \(seq.minValue)"
        sql += "\n    MAXVALUE \(seq.maxValue)"
        sql += "\n    CACHE \(seq.cacheSize)"
        sql += seq.isCycled ? "\n    CYCLE" : "\n    NO CYCLE"
        sql += ";\n"

        if let ownedBy = seq.ownedByColumn {
            sql += "\nALTER SEQUENCE \(seq.id.qualifiedName) OWNED BY \(ownedBy);\n"
        }

        return sql
    }
}
