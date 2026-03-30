/// Generates CREATE TABLE DDL from introspected table metadata.
public struct TableSQLGenerator: SQLGenerator {
    public var supportedTypes: [ObjectType] { [.table] }

    public init() {}

    public func generateCreate(from metadata: any ObjectMetadata) throws -> String {
        guard let table = metadata as? TableMetadata else {
            throw PGSchemaEvoError.sqlGenerationFailed(
                metadata.id,
                reason: "Expected TableMetadata, got \(type(of: metadata))"
            )
        }
        return generateCreateTable(table)
    }

    public func generateDrop(for id: ObjectIdentifier) -> String {
        "DROP TABLE IF EXISTS \(id.qualifiedName) CASCADE;"
    }

    // MARK: - Private

    private func generateCreateTable(_ table: TableMetadata) -> String {
        var sql = "CREATE TABLE \(table.id.qualifiedName) (\n"

        // Columns
        let columnDefs = table.columns
            .sorted { $0.ordinalPosition < $1.ordinalPosition }
            .map { columnDefinition($0) }

        // Inline constraints (primary key, unique, check, exclusion)
        let constraintDefs = table.constraints
            .filter { $0.type != .foreignKey }
            .map { constraintDefinition($0) }

        let allDefs = columnDefs + constraintDefs
        sql += allDefs.map { "    \($0)" }.joined(separator: ",\n")
        sql += "\n);\n"

        // Foreign key constraints (separate ALTER TABLE for clarity and dependency ordering)
        let fkConstraints = table.constraints.filter { $0.type == .foreignKey }
        for fk in fkConstraints {
            sql += "\nALTER TABLE \(table.id.qualifiedName)\n"
            sql += "    ADD CONSTRAINT \(quoteIdent(fk.name)) \(fk.definition);\n"
        }

        // Non-primary, non-unique-constraint indexes (unique constraint indexes are implicit)
        let explicitIndexes = table.indexes.filter { !$0.isPrimary && !isConstraintIndex($0, constraints: table.constraints) }
        for index in explicitIndexes {
            sql += "\n\(index.definition);\n"
        }

        // Triggers
        for trigger in table.triggers {
            sql += "\n\(trigger.definition);\n"
        }

        return sql
    }

    private func columnDefinition(_ col: ColumnInfo) -> String {
        var def = "\(quoteIdent(col.name)) \(col.dataType)"

        if col.isIdentity, let gen = col.identityGeneration {
            def += " GENERATED \(gen) AS IDENTITY"
        } else if let defaultVal = col.columnDefault {
            def += " DEFAULT \(defaultVal)"
        }

        if !col.isNullable {
            def += " NOT NULL"
        }

        return def
    }

    private func constraintDefinition(_ constraint: ConstraintInfo) -> String {
        "CONSTRAINT \(quoteIdent(constraint.name)) \(constraint.definition)"
    }

    /// Check if an index is implicitly created by a constraint (e.g., primary key or unique).
    private func isConstraintIndex(_ index: IndexInfo, constraints: [ConstraintInfo]) -> Bool {
        constraints.contains { $0.name == index.name }
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}
