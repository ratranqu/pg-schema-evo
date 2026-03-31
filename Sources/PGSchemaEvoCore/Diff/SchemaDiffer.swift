import Logging

/// Compares schemas between source and target databases.
/// Produces both a human-readable diff and a SQL migration script.
public struct SchemaDiffer: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Compare objects between source and target for the given types/schema filter.
    public func diff(
        source: SchemaIntrospector,
        target: SchemaIntrospector,
        schema: String? = nil,
        types: [ObjectType]? = nil
    ) async throws -> SchemaDiff {
        let sourceObjects = try await source.listObjects(schema: schema, types: types)
        let targetObjects = try await target.listObjects(schema: schema, types: types)

        let sourceSet = Set(sourceObjects)
        let targetSet = Set(targetObjects)

        let onlyInSource = sourceSet.subtracting(targetSet).sorted { $0.description < $1.description }
        let onlyInTarget = targetSet.subtracting(sourceSet).sorted { $0.description < $1.description }
        let inBoth = sourceSet.intersection(targetSet).sorted { $0.description < $1.description }

        // For objects present in both, compare definitions
        var modified: [ObjectDiff] = []
        for id in inBoth {
            if let objDiff = try await compareObject(id, source: source, target: target) {
                modified.append(objDiff)
            }
        }

        return SchemaDiff(
            onlyInSource: Array(onlyInSource),
            onlyInTarget: Array(onlyInTarget),
            modified: modified,
            matching: inBoth.count - modified.count
        )
    }

    private func compareObject(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        switch id.type {
        case .table:
            return try await compareTable(id, source: source, target: target)
        case .view, .materializedView:
            return try await compareView(id, source: source, target: target)
        case .sequence:
            return try await compareSequence(id, source: source, target: target)
        case .enum:
            return try await compareEnum(id, source: source, target: target)
        case .function, .procedure:
            return try await compareFunction(id, source: source, target: target)
        default:
            return nil
        }
    }

    private func compareTable(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeTable(id)
        let tgtMeta = try await target.describeTable(id)

        var differences: [String] = []
        var migrationSQL: [String] = []

        // Compare columns
        let srcCols = Dictionary(uniqueKeysWithValues: srcMeta.columns.map { ($0.name, $0) })
        let tgtCols = Dictionary(uniqueKeysWithValues: tgtMeta.columns.map { ($0.name, $0) })

        for (name, srcCol) in srcCols {
            if let tgtCol = tgtCols[name] {
                if srcCol.dataType != tgtCol.dataType {
                    differences.append("Column \(name): type \(tgtCol.dataType) -> \(srcCol.dataType)")
                    migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(srcCol.dataType);")
                }
                if srcCol.isNullable != tgtCol.isNullable {
                    let change = srcCol.isNullable ? "DROP NOT NULL" : "SET NOT NULL"
                    differences.append("Column \(name): nullability changed")
                    migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) \(change);")
                }
                if srcCol.columnDefault != tgtCol.columnDefault {
                    differences.append("Column \(name): default changed")
                    if let def = srcCol.columnDefault {
                        migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) SET DEFAULT \(def);")
                    } else {
                        migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) DROP DEFAULT;")
                    }
                }
            } else {
                differences.append("Column \(name): missing in target (type: \(srcCol.dataType))")
                var colDef = "\(quoteIdent(name)) \(srcCol.dataType)"
                if !srcCol.isNullable { colDef += " NOT NULL" }
                if let def = srcCol.columnDefault { colDef += " DEFAULT \(def)" }
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ADD COLUMN \(colDef);")
            }
        }

        for (name, _) in tgtCols where srcCols[name] == nil {
            differences.append("Column \(name): extra in target (not in source)")
        }

        // Compare constraints
        let srcConstraints = Set(srcMeta.constraints.map(\.name))
        let tgtConstraints = Set(tgtMeta.constraints.map(\.name))
        for name in srcConstraints.subtracting(tgtConstraints) {
            if let con = srcMeta.constraints.first(where: { $0.name == name }) {
                differences.append("Constraint \(name): missing in target")
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ADD CONSTRAINT \(quoteIdent(name)) \(con.definition);")
            }
        }
        for name in tgtConstraints.subtracting(srcConstraints) {
            differences.append("Constraint \(name): extra in target")
        }

        // Compare indexes
        let srcIndexes = Set(srcMeta.indexes.map(\.name))
        let tgtIndexes = Set(tgtMeta.indexes.map(\.name))
        for name in srcIndexes.subtracting(tgtIndexes) {
            if let idx = srcMeta.indexes.first(where: { $0.name == name }) {
                differences.append("Index \(name): missing in target")
                migrationSQL.append("\(idx.definition);")
            }
        }
        for name in tgtIndexes.subtracting(srcIndexes) {
            differences.append("Index \(name): extra in target")
        }

        guard !differences.isEmpty else { return nil }

        return ObjectDiff(
            id: id,
            differences: differences,
            migrationSQL: migrationSQL
        )
    }

    private func compareView(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcDef: String
        let tgtDef: String

        if id.type == .view {
            let srcMeta = try await source.describeView(id)
            let tgtMeta = try await target.describeView(id)
            srcDef = srcMeta.definition.trimmingCharacters(in: .whitespacesAndNewlines)
            tgtDef = tgtMeta.definition.trimmingCharacters(in: .whitespacesAndNewlines)
        } else {
            let srcMeta = try await source.describeMaterializedView(id)
            let tgtMeta = try await target.describeMaterializedView(id)
            srcDef = srcMeta.definition.trimmingCharacters(in: .whitespacesAndNewlines)
            tgtDef = tgtMeta.definition.trimmingCharacters(in: .whitespacesAndNewlines)
        }

        guard srcDef != tgtDef else { return nil }

        let keyword = id.type == .view ? "VIEW" : "MATERIALIZED VIEW"
        return ObjectDiff(
            id: id,
            differences: ["Definition differs"],
            migrationSQL: ["CREATE OR REPLACE \(keyword) \(id.qualifiedName) AS\n\(srcDef);"]
        )
    }

    private func compareSequence(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeSequence(id)
        let tgtMeta = try await target.describeSequence(id)

        var diffs: [String] = []
        var sql: [String] = []
        var alterParts: [String] = []

        if srcMeta.increment != tgtMeta.increment {
            diffs.append("INCREMENT: \(tgtMeta.increment) -> \(srcMeta.increment)")
            alterParts.append("INCREMENT BY \(srcMeta.increment)")
        }
        if srcMeta.minValue != tgtMeta.minValue {
            diffs.append("MIN: \(tgtMeta.minValue) -> \(srcMeta.minValue)")
            alterParts.append("MINVALUE \(srcMeta.minValue)")
        }
        if srcMeta.maxValue != tgtMeta.maxValue {
            diffs.append("MAX: \(tgtMeta.maxValue) -> \(srcMeta.maxValue)")
            alterParts.append("MAXVALUE \(srcMeta.maxValue)")
        }
        if srcMeta.cacheSize != tgtMeta.cacheSize {
            diffs.append("CACHE: \(tgtMeta.cacheSize) -> \(srcMeta.cacheSize)")
            alterParts.append("CACHE \(srcMeta.cacheSize)")
        }
        if srcMeta.isCycled != tgtMeta.isCycled {
            diffs.append("CYCLE: \(tgtMeta.isCycled ? "YES" : "NO") -> \(srcMeta.isCycled ? "YES" : "NO")")
            alterParts.append(srcMeta.isCycled ? "CYCLE" : "NO CYCLE")
        }

        if !alterParts.isEmpty {
            sql.append("ALTER SEQUENCE \(id.qualifiedName) \(alterParts.joined(separator: " "));")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql)
    }

    private func compareEnum(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeEnum(id)
        let tgtMeta = try await target.describeEnum(id)

        let srcLabels = srcMeta.labels
        let tgtLabels = tgtMeta.labels

        guard srcLabels != tgtLabels else { return nil }

        var diffs: [String] = []
        var sql: [String] = []

        let missingInTarget = srcLabels.filter { !tgtLabels.contains($0) }
        let extraInTarget = tgtLabels.filter { !srcLabels.contains($0) }

        for label in missingInTarget {
            diffs.append("Label '\(label)': missing in target")
            // Find position: add after the preceding label or at end
            if let idx = srcLabels.firstIndex(of: label), idx > 0 {
                let before = srcLabels[idx - 1]
                sql.append("ALTER TYPE \(id.qualifiedName) ADD VALUE '\(escapeSQLString(label))' AFTER '\(escapeSQLString(before))';")
            } else {
                sql.append("ALTER TYPE \(id.qualifiedName) ADD VALUE '\(escapeSQLString(label))';")
            }
        }
        for label in extraInTarget {
            diffs.append("Label '\(label)': extra in target (cannot remove enum values in PostgreSQL)")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql)
    }

    private func compareFunction(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeFunction(id)
        let tgtMeta = try await target.describeFunction(id)

        // Compare using the full function definition
        let srcDef = srcMeta.definition.trimmingCharacters(in: .whitespacesAndNewlines)
        let tgtDef = tgtMeta.definition.trimmingCharacters(in: .whitespacesAndNewlines)

        guard srcDef != tgtDef else { return nil }

        return ObjectDiff(
            id: id,
            differences: ["Function definition differs"],
            migrationSQL: [srcDef + ";"]
        )
    }

    private func quoteIdent(_ ident: String) -> String {
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    private func escapeSQLString(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}

// MARK: - Diff Result Types

public struct SchemaDiff: Sendable {
    /// Objects present in source but missing from target.
    public let onlyInSource: [ObjectIdentifier]
    /// Objects present in target but missing from source.
    public let onlyInTarget: [ObjectIdentifier]
    /// Objects present in both but with differences.
    public let modified: [ObjectDiff]
    /// Number of identical objects.
    public let matching: Int

    public var isEmpty: Bool {
        onlyInSource.isEmpty && onlyInTarget.isEmpty && modified.isEmpty
    }

    /// Render as a human-readable text diff.
    public func renderText() -> String {
        var lines: [String] = []

        if isEmpty {
            lines.append("Schemas are identical.")
            return lines.joined(separator: "\n")
        }

        if !onlyInSource.isEmpty {
            lines.append("Objects only in source (\(onlyInSource.count)):")
            for id in onlyInSource {
                lines.append("  + \(id)")
            }
            lines.append("")
        }

        if !onlyInTarget.isEmpty {
            lines.append("Objects only in target (\(onlyInTarget.count)):")
            for id in onlyInTarget {
                lines.append("  - \(id)")
            }
            lines.append("")
        }

        if !modified.isEmpty {
            lines.append("Modified objects (\(modified.count)):")
            for objDiff in modified {
                lines.append("  ~ \(objDiff.id)")
                for diff in objDiff.differences {
                    lines.append("      \(diff)")
                }
            }
            lines.append("")
        }

        lines.append("Summary: \(matching) matching, \(onlyInSource.count) only in source, \(onlyInTarget.count) only in target, \(modified.count) modified")

        return lines.joined(separator: "\n")
    }

    /// Render as a SQL migration script to bring target in sync with source.
    public func renderMigrationSQL() -> String {
        var sql: [String] = []

        sql.append("-- pg-schema-evo migration script")
        sql.append("-- Brings target in sync with source")
        sql.append("BEGIN;")
        sql.append("")

        // Modified objects first
        for objDiff in modified {
            sql.append("-- Modify \(objDiff.id)")
            sql.append(contentsOf: objDiff.migrationSQL)
            sql.append("")
        }

        // Note: creating missing objects requires introspection of source DDL
        // which is handled by the clone command. Here we just flag them.
        if !onlyInSource.isEmpty {
            sql.append("-- Objects missing in target (use 'clone' command to create):")
            for id in onlyInSource {
                sql.append("-- CREATE \(id)")
            }
            sql.append("")
        }

        sql.append("COMMIT;")

        return sql.joined(separator: "\n")
    }
}

public struct ObjectDiff: Sendable {
    public let id: ObjectIdentifier
    public let differences: [String]
    public let migrationSQL: [String]
}
