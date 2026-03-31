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
        case .compositeType:
            return try await compareCompositeType(id, source: source, target: target)
        case .schema:
            return try await compareSchema(id, source: source, target: target)
        case .role:
            return try await compareRole(id, source: source, target: target)
        case .extension:
            return try await compareExtension(id, source: source, target: target)
        default:
            // Aggregates, operators, FDWs, foreign tables: no structured comparison yet
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

    private func compareCompositeType(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeCompositeType(id)
        let tgtMeta = try await target.describeCompositeType(id)

        let srcAttrs = Dictionary(uniqueKeysWithValues: srcMeta.attributes.map { ($0.name, $0) })
        let tgtAttrs = Dictionary(uniqueKeysWithValues: tgtMeta.attributes.map { ($0.name, $0) })

        var diffs: [String] = []
        var sql: [String] = []

        for (name, srcAttr) in srcAttrs {
            if let tgtAttr = tgtAttrs[name] {
                if srcAttr.dataType != tgtAttr.dataType {
                    diffs.append("Attribute \(name): type \(tgtAttr.dataType) -> \(srcAttr.dataType)")
                    sql.append("ALTER TYPE \(id.qualifiedName) ALTER ATTRIBUTE \(quoteIdent(name)) TYPE \(srcAttr.dataType);")
                }
            } else {
                diffs.append("Attribute \(name): missing in target (type: \(srcAttr.dataType))")
                sql.append("ALTER TYPE \(id.qualifiedName) ADD ATTRIBUTE \(quoteIdent(name)) \(srcAttr.dataType);")
            }
        }

        for (name, _) in tgtAttrs where srcAttrs[name] == nil {
            diffs.append("Attribute \(name): extra in target")
            sql.append("ALTER TYPE \(id.qualifiedName) DROP ATTRIBUTE \(quoteIdent(name));")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql)
    }

    private func compareSchema(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeSchema(id)
        let tgtMeta = try await target.describeSchema(id)

        guard srcMeta.owner != tgtMeta.owner else { return nil }

        return ObjectDiff(
            id: id,
            differences: ["Owner: \(tgtMeta.owner) -> \(srcMeta.owner)"],
            migrationSQL: ["ALTER SCHEMA \(id.qualifiedName) OWNER TO \(quoteIdent(srcMeta.owner));"]
        )
    }

    private func compareRole(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeRole(id)
        let tgtMeta = try await target.describeRole(id)

        var diffs: [String] = []
        var alterParts: [String] = []

        if srcMeta.canLogin != tgtMeta.canLogin {
            diffs.append("LOGIN: \(tgtMeta.canLogin) -> \(srcMeta.canLogin)")
            alterParts.append(srcMeta.canLogin ? "LOGIN" : "NOLOGIN")
        }
        if srcMeta.isSuperuser != tgtMeta.isSuperuser {
            diffs.append("SUPERUSER: \(tgtMeta.isSuperuser) -> \(srcMeta.isSuperuser)")
            alterParts.append(srcMeta.isSuperuser ? "SUPERUSER" : "NOSUPERUSER")
        }
        if srcMeta.canCreateDB != tgtMeta.canCreateDB {
            diffs.append("CREATEDB: \(tgtMeta.canCreateDB) -> \(srcMeta.canCreateDB)")
            alterParts.append(srcMeta.canCreateDB ? "CREATEDB" : "NOCREATEDB")
        }
        if srcMeta.canCreateRole != tgtMeta.canCreateRole {
            diffs.append("CREATEROLE: \(tgtMeta.canCreateRole) -> \(srcMeta.canCreateRole)")
            alterParts.append(srcMeta.canCreateRole ? "CREATEROLE" : "NOCREATEROLE")
        }
        if srcMeta.connectionLimit != tgtMeta.connectionLimit {
            diffs.append("CONNECTION LIMIT: \(tgtMeta.connectionLimit) -> \(srcMeta.connectionLimit)")
            alterParts.append("CONNECTION LIMIT \(srcMeta.connectionLimit)")
        }

        var sql: [String] = []
        if !alterParts.isEmpty {
            sql.append("ALTER ROLE \(quoteIdent(id.name)) \(alterParts.joined(separator: " "));")
        }

        // Membership changes
        let srcMembers = Set(srcMeta.memberOf)
        let tgtMembers = Set(tgtMeta.memberOf)
        for role in srcMembers.subtracting(tgtMembers) {
            diffs.append("Membership: missing GRANT \(role)")
            sql.append("GRANT \(quoteIdent(role)) TO \(quoteIdent(id.name));")
        }
        for role in tgtMembers.subtracting(srcMembers) {
            diffs.append("Membership: extra GRANT \(role)")
            sql.append("REVOKE \(quoteIdent(role)) FROM \(quoteIdent(id.name));")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql)
    }

    private func compareExtension(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        let srcMeta = try await source.describeExtension(id)
        let tgtMeta = try await target.describeExtension(id)

        guard srcMeta.version != tgtMeta.version else { return nil }

        return ObjectDiff(
            id: id,
            differences: ["Version: \(tgtMeta.version) -> \(srcMeta.version)"],
            migrationSQL: ["ALTER EXTENSION \(quoteIdent(id.name)) UPDATE TO '\(escapeSQLString(srcMeta.version))';"]
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
