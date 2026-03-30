/// Generates DDL for views and materialized views.
public struct ViewSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.view, .materializedView] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        if let view = metadata as? ViewMetadata {
            return generateCreateView(view)
        } else if let matview = metadata as? MaterializedViewMetadata {
            return generateCreateMaterializedView(matview)
        }
        throw PGSchemaEvoError.sqlGenerationFailed(
            metadata.id,
            reason: "Expected ViewMetadata or MaterializedViewMetadata"
        )
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        switch id.type {
        case .materializedView:
            return "DROP MATERIALIZED VIEW IF EXISTS \(id.qualifiedName) CASCADE;"
        default:
            return "DROP VIEW IF EXISTS \(id.qualifiedName) CASCADE;"
        }
    }

    private func generateCreateView(_ view: ViewMetadata) -> String {
        """
        CREATE OR REPLACE VIEW \(view.id.qualifiedName) AS
        \(view.definition);
        """
    }

    private func generateCreateMaterializedView(_ matview: MaterializedViewMetadata) -> String {
        var sql = """
            CREATE MATERIALIZED VIEW \(matview.id.qualifiedName) AS
            \(matview.definition)
            WITH DATA;
            """

        for index in matview.indexes {
            sql += "\n\n\(index.definition);"
        }

        return sql
    }
}
