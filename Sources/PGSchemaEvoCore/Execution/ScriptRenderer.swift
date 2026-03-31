import Foundation

/// Renders a dry-run clone operation as an executable bash script.
public struct ScriptRenderer: Sendable {

    public init() {}

    /// Render a complete bash script for the given clone steps.
    public func render(
        job: CloneJob,
        steps: [CloneStep]
    ) -> String {
        var script = """
            #!/usr/bin/env bash
            set -euo pipefail

            # pg-schema-evo dry-run output
            # Generated: \(ISO8601DateFormatter().string(from: Date()))
            # Source: \(job.source.toDSN(maskPassword: true))
            # Target: \(job.target.toDSN(maskPassword: true))

            TARGET_DSN="\(job.target.toDSN())"
            SOURCE_DSN="\(job.source.toDSN())"

            """

        for (index, step) in steps.enumerated() {
            script += "\n"
            script += renderStep(step, number: index + 1, job: job)
        }

        return script
    }

    // MARK: - Private

    private func renderStep(_ step: CloneStep, number: Int, job: CloneJob) -> String {
        var section = ""

        switch step {
        case .dropObject(let id):
            section += sectionHeader(number, "Drop \(id.type.displayName): \(id)")
            section += wrapInPsql(
                "DROP \(sqlObjectType(id.type)) IF EXISTS \(id.qualifiedName) CASCADE;",
                target: "$TARGET_DSN"
            )

        case .createObject(let sql, let id):
            section += sectionHeader(number, "Create \(id.type.displayName): \(id)")
            section += wrapInPsql(sql, target: "$TARGET_DSN")

        case .alterObject(let sql, let id):
            section += sectionHeader(number, "Alter \(id.type.displayName): \(id)")
            section += wrapInPsql(sql, target: "$TARGET_DSN")

        case .copyData(let id, let method, let estimatedSize, let whereClause, let rowLimit):
            let sizeStr = estimatedSize.map { formatBytes($0) } ?? "unknown size"
            var desc = "Copy data: \(id) (estimated \(sizeStr), method: \(method.rawValue))"
            if let wh = whereClause { desc += " WHERE \(wh)" }
            if let lim = rowLimit { desc += " LIMIT \(lim)" }
            section += sectionHeader(number, desc)

            switch method {
            case .copy, .auto:
                section += copyViaPsql(id, whereClause: whereClause, rowLimit: rowLimit)
            case .pgDump:
                section += copyViaPgDump(id)
            }

        case .grantPermissions(let sql, let id):
            section += sectionHeader(number, "Permissions: \(id)")
            section += wrapInPsql(sql, target: "$TARGET_DSN")

        case .refreshMaterializedView(let id):
            section += sectionHeader(number, "Refresh materialized view: \(id)")
            section += wrapInPsql(
                "REFRESH MATERIALIZED VIEW \(id.qualifiedName);",
                target: "$TARGET_DSN"
            )

        case .enableRLS(let sql, let id):
            section += sectionHeader(number, "Enable RLS: \(id)")
            section += wrapInPsql(sql, target: "$TARGET_DSN")

        case .attachPartition(let sql, let id):
            section += sectionHeader(number, "Attach partition: \(id)")
            section += wrapInPsql(sql, target: "$TARGET_DSN")
        }

        return section
    }

    private func sectionHeader(_ number: Int, _ title: String) -> String {
        """
        #---------------------------------------
        # \(number). \(title)
        #---------------------------------------

        """
    }

    private func wrapInPsql(_ sql: String, target: String) -> String {
        """
        psql "\(target)" <<'EOSQL'
        \(sql)
        EOSQL

        """
    }

    private func copyViaPsql(_ id: ObjectIdentifier, whereClause: String? = nil, rowLimit: Int? = nil) -> String {
        // When WHERE or LIMIT is specified, use SQL COPY with a subquery
        if whereClause != nil || rowLimit != nil {
            var query = "SELECT * FROM \(id.qualifiedName)"
            if let wh = whereClause { query += " WHERE \(wh)" }
            if let lim = rowLimit { query += " LIMIT \(lim)" }
            return """
                psql "$SOURCE_DSN" \\
                  -c "\\copy (\(query)) TO STDOUT WITH (FORMAT csv, HEADER)" \\
                | psql "$TARGET_DSN" \\
                  -c "\\copy \(id.qualifiedName) FROM STDIN WITH (FORMAT csv, HEADER)"

                """
        }
        return """
            psql "$SOURCE_DSN" \\
              -c "\\copy \(id.qualifiedName) TO STDOUT WITH (FORMAT csv, HEADER)" \\
            | psql "$TARGET_DSN" \\
              -c "\\copy \(id.qualifiedName) FROM STDIN WITH (FORMAT csv, HEADER)"

            """
    }

    private func copyViaPgDump(_ id: ObjectIdentifier) -> String {
        """
        pg_dump --format=custom --data-only --table=\(id.qualifiedName) "$SOURCE_DSN" \\
        | pg_restore --no-owner --data-only --dbname="$TARGET_DSN"

        """
    }

    private func sqlObjectType(_ type: ObjectType) -> String {
        switch type {
        case .table: "TABLE"
        case .view: "VIEW"
        case .materializedView: "MATERIALIZED VIEW"
        case .sequence: "SEQUENCE"
        case .function: "FUNCTION"
        case .procedure: "PROCEDURE"
        case .schema: "SCHEMA"
        case .extension: "EXTENSION"
        case .enum, .compositeType: "TYPE"
        case .role: "ROLE"
        case .aggregate: "AGGREGATE"
        case .operator: "OPERATOR"
        case .foreignDataWrapper: "FOREIGN DATA WRAPPER"
        case .foreignTable: "FOREIGN TABLE"
        }
    }

    private func formatBytes(_ bytes: Int) -> String {
        if bytes >= 1_073_741_824 {
            return String(format: "%.1f GB", Double(bytes) / 1_073_741_824.0)
        } else if bytes >= 1_048_576 {
            return String(format: "%.1f MB", Double(bytes) / 1_048_576.0)
        } else if bytes >= 1024 {
            return String(format: "%.1f KB", Double(bytes) / 1024.0)
        }
        return "\(bytes) B"
    }
}

/// A single step in a clone operation, used by both ScriptRenderer and LiveExecutor.
public enum CloneStep: Sendable {
    case dropObject(ObjectIdentifier)
    case createObject(sql: String, id: ObjectIdentifier)
    case alterObject(sql: String, id: ObjectIdentifier)
    case copyData(id: ObjectIdentifier, method: TransferMethod, estimatedSize: Int?, whereClause: String? = nil, rowLimit: Int? = nil)
    case grantPermissions(sql: String, id: ObjectIdentifier)
    case refreshMaterializedView(ObjectIdentifier)
    case enableRLS(sql: String, id: ObjectIdentifier)
    case attachPartition(sql: String, id: ObjectIdentifier)
}
