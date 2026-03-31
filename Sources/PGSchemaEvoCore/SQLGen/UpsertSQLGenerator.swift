/// Generates SQL for incremental data sync operations (UPSERT, DELETE orphans).
public struct UpsertSQLGenerator: Sendable {

    public init() {}

    /// Generate the UPSERT SQL that moves data from a temp table into the target.
    ///
    /// Produces:
    /// ```sql
    /// INSERT INTO "schema"."table" SELECT * FROM _sync_tmp_name
    ///   ON CONFLICT ("pk1", "pk2") DO UPDATE SET
    ///     "col1" = EXCLUDED."col1", "col2" = EXCLUDED."col2", ...;
    /// ```
    public func generateUpsertSQL(
        table: ObjectIdentifier,
        columns: [ColumnInfo],
        pkColumns: [String],
        tempTableName: String
    ) -> String {
        let pkSet = Set(pkColumns)
        let nonPKColumns = columns.filter { !pkSet.contains($0.name) }

        let conflictTarget = pkColumns.map { quoteIdent($0) }.joined(separator: ", ")

        if nonPKColumns.isEmpty {
            // All columns are part of the PK — INSERT with ON CONFLICT DO NOTHING
            return """
                INSERT INTO \(table.qualifiedName) SELECT * FROM \(quoteIdent(tempTableName))
                  ON CONFLICT (\(conflictTarget)) DO NOTHING;
                """
        }

        let setClause = nonPKColumns.map { col in
            "\(quoteIdent(col.name)) = EXCLUDED.\(quoteIdent(col.name))"
        }.joined(separator: ",\n    ")

        return """
            INSERT INTO \(table.qualifiedName) SELECT * FROM \(quoteIdent(tempTableName))
              ON CONFLICT (\(conflictTarget)) DO UPDATE SET
              \(setClause);
            """
    }

    /// Generate SQL to delete rows from the target that no longer exist in the source.
    ///
    /// Uses a temp table containing all source PKs and deletes target rows not found there.
    public func generateDeleteOrphansSQL(
        table: ObjectIdentifier,
        pkColumns: [String],
        deleteTempTableName: String
    ) -> String {
        if pkColumns.count == 1 {
            let pk = quoteIdent(pkColumns[0])
            return """
                DELETE FROM \(table.qualifiedName) t
                WHERE NOT EXISTS (
                  SELECT 1 FROM \(quoteIdent(deleteTempTableName)) s
                  WHERE s.\(pk) = t.\(pk)
                );
                """
        }

        let joinCondition = pkColumns.map { col in
            "s.\(quoteIdent(col)) = t.\(quoteIdent(col))"
        }.joined(separator: " AND ")

        return """
            DELETE FROM \(table.qualifiedName) t
            WHERE NOT EXISTS (
              SELECT 1 FROM \(quoteIdent(deleteTempTableName)) s
              WHERE \(joinCondition)
            );
            """
    }

    /// Generate the query to fetch the current maximum tracking value from a table.
    public func generateMaxTrackingQuery(
        table: ObjectIdentifier,
        trackingColumn: String
    ) -> String {
        "SELECT MAX(\(quoteIdent(trackingColumn)))::text FROM \(table.qualifiedName)"
    }

    /// Generate the COPY command to export incremental rows from the source.
    public func generateIncrementalCopyCommand(
        table: ObjectIdentifier,
        trackingColumn: String,
        lastValue: String
    ) -> String {
        let query = "SELECT * FROM \(table.qualifiedName) WHERE \(quoteIdent(trackingColumn)) > '\(escapeLiteral(lastValue))' ORDER BY \(quoteIdent(trackingColumn))"
        return "\\copy (\(query)) TO STDOUT WITH (FORMAT csv, HEADER)"
    }

    /// Generate the COPY command to export all primary key values from the source (for delete detection).
    public func generatePKExportCommand(
        table: ObjectIdentifier,
        pkColumns: [String]
    ) -> String {
        let cols = pkColumns.map { quoteIdent($0) }.joined(separator: ", ")
        return "\\copy (SELECT \(cols) FROM \(table.qualifiedName)) TO STDOUT WITH (FORMAT csv, HEADER)"
    }

    /// Build a complete transaction script for one table's incremental sync.
    public func buildTableSyncScript(
        table: ObjectIdentifier,
        columns: [ColumnInfo],
        pkColumns: [String],
        csvData: String,
        detectDeletes: Bool,
        deletePKData: String?
    ) -> String {
        let tempName = "_sync_tmp_\(table.schema ?? "public")_\(table.name)"
        let deleteTempName = "_sync_del_\(table.schema ?? "public")_\(table.name)"

        var script = "BEGIN;\n\n"

        // Create temp table for UPSERT data
        script += "CREATE TEMP TABLE \(quoteIdent(tempName)) (LIKE \(table.qualifiedName) INCLUDING DEFAULTS) ON COMMIT DROP;\n\n"

        // COPY data into temp table
        script += "COPY \(quoteIdent(tempName)) FROM STDIN WITH (FORMAT csv, HEADER);\n"
        script += csvData
        if !csvData.hasSuffix("\n") { script += "\n" }
        script += "\\.\n\n"

        // UPSERT from temp into target
        script += generateUpsertSQL(
            table: table,
            columns: columns,
            pkColumns: pkColumns,
            tempTableName: tempName
        )
        script += "\n\n"

        // Optional delete detection
        if detectDeletes, let deletePKData, !deletePKData.isEmpty {
            let pkCols = pkColumns.map { quoteIdent($0) }.joined(separator: ", ")
            script += "CREATE TEMP TABLE \(quoteIdent(deleteTempName)) AS SELECT \(pkCols) FROM \(table.qualifiedName) LIMIT 0;\n\n"

            script += "COPY \(quoteIdent(deleteTempName)) FROM STDIN WITH (FORMAT csv, HEADER);\n"
            script += deletePKData
            if !deletePKData.hasSuffix("\n") { script += "\n" }
            script += "\\.\n\n"

            script += generateDeleteOrphansSQL(
                table: table,
                pkColumns: pkColumns,
                deleteTempTableName: deleteTempName
            )
            script += "\n\n"
        }

        script += "COMMIT;\n"
        return script
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    private func escapeLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}
