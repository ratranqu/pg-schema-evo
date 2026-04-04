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

    /// Compare a single object between source and target databases.
    public func compareObjects(
        _ id: ObjectIdentifier,
        source: SchemaIntrospector,
        target: SchemaIntrospector
    ) async throws -> ObjectDiff? {
        try await compareObject(id, source: source, target: target)
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
        var dropColumnSQL: [String] = []
        var reverseMigrationSQL: [String] = []
        var reverseDropColumnSQL: [String] = []

        // Compare columns
        let srcCols = Dictionary(uniqueKeysWithValues: srcMeta.columns.map { ($0.name, $0) })
        let tgtCols = Dictionary(uniqueKeysWithValues: tgtMeta.columns.map { ($0.name, $0) })

        for (name, srcCol) in srcCols {
            if let tgtCol = tgtCols[name] {
                if srcCol.dataType != tgtCol.dataType {
                    differences.append("Column \(name): type \(tgtCol.dataType) -> \(srcCol.dataType)")
                    migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(srcCol.dataType);")
                    reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(tgtCol.dataType);")
                }
                if srcCol.isNullable != tgtCol.isNullable {
                    let change = srcCol.isNullable ? "DROP NOT NULL" : "SET NOT NULL"
                    let reverseChange = srcCol.isNullable ? "SET NOT NULL" : "DROP NOT NULL"
                    differences.append("Column \(name): nullability changed")
                    migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) \(change);")
                    reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) \(reverseChange);")
                }
                if srcCol.columnDefault != tgtCol.columnDefault {
                    differences.append("Column \(name): default changed")
                    if let def = srcCol.columnDefault {
                        migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) SET DEFAULT \(def);")
                    } else {
                        migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) DROP DEFAULT;")
                    }
                    if let def = tgtCol.columnDefault {
                        reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) SET DEFAULT \(def);")
                    } else {
                        reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) DROP DEFAULT;")
                    }
                }
                // Compare identity columns
                if srcCol.isIdentity != tgtCol.isIdentity {
                    if srcCol.isIdentity {
                        let gen = srcCol.identityGeneration ?? "ALWAYS"
                        differences.append("Column \(name): identity added (GENERATED \(gen))")
                        migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) ADD GENERATED \(gen) AS IDENTITY;")
                        reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) DROP IDENTITY;")
                    } else {
                        let gen = tgtCol.identityGeneration ?? "ALWAYS"
                        differences.append("Column \(name): identity removed (was GENERATED \(gen))")
                        migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) DROP IDENTITY;")
                        reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) ADD GENERATED \(gen) AS IDENTITY;")
                    }
                } else if srcCol.isIdentity && srcCol.identityGeneration != tgtCol.identityGeneration {
                    let srcGen = srcCol.identityGeneration ?? "ALWAYS"
                    let tgtGen = tgtCol.identityGeneration ?? "ALWAYS"
                    differences.append("Column \(name): identity generation \(tgtGen) -> \(srcGen)")
                    migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) SET GENERATED \(srcGen);")
                    reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) SET GENERATED \(tgtGen);")
                }
                // Compare character length and numeric precision/scale
                if srcCol.characterMaximumLength != tgtCol.characterMaximumLength,
                   srcCol.characterMaximumLength != nil || tgtCol.characterMaximumLength != nil {
                    let srcLen = srcCol.characterMaximumLength.map(String.init) ?? "unlimited"
                    let tgtLen = tgtCol.characterMaximumLength.map(String.init) ?? "unlimited"
                    differences.append("Column \(name): character max length \(tgtLen) -> \(srcLen)")
                    // Generate ALTER TYPE only when dataType comparison didn't already handle it
                    if srcCol.dataType == tgtCol.dataType {
                        if let len = srcCol.characterMaximumLength {
                            migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(srcCol.dataType)(\(len));")
                        } else {
                            migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(srcCol.dataType);")
                        }
                        if let rLen = tgtCol.characterMaximumLength {
                            reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(tgtCol.dataType)(\(rLen));")
                        } else {
                            reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(tgtCol.dataType);")
                        }
                    }
                }
                if srcCol.numericPrecision != tgtCol.numericPrecision || srcCol.numericScale != tgtCol.numericScale {
                    let hasPrecisionDiff = srcCol.numericPrecision != nil || tgtCol.numericPrecision != nil
                    if hasPrecisionDiff {
                        let srcPrec = srcCol.numericPrecision.map(String.init) ?? "default"
                        let tgtPrec = tgtCol.numericPrecision.map(String.init) ?? "default"
                        let srcScale = srcCol.numericScale.map(String.init) ?? "default"
                        let tgtScale = tgtCol.numericScale.map(String.init) ?? "default"
                        differences.append("Column \(name): numeric precision/scale (\(tgtPrec),\(tgtScale)) -> (\(srcPrec),\(srcScale))")
                        // Generate ALTER TYPE only when dataType comparison didn't already handle it
                        if srcCol.dataType == tgtCol.dataType {
                            if let p = srcCol.numericPrecision {
                                let scaleClause = srcCol.numericScale.map { ",\($0)" } ?? ""
                                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(srcCol.dataType)(\(p)\(scaleClause));")
                            } else {
                                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(srcCol.dataType);")
                            }
                            if let rp = tgtCol.numericPrecision {
                                let rScaleClause = tgtCol.numericScale.map { ",\($0)" } ?? ""
                                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(tgtCol.dataType)(\(rp)\(rScaleClause));")
                            } else {
                                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ALTER COLUMN \(quoteIdent(name)) TYPE \(tgtCol.dataType);")
                            }
                        }
                    }
                }
            } else {
                differences.append("Column \(name): missing in target (type: \(srcCol.dataType))")
                var colDef = "\(quoteIdent(name)) \(srcCol.dataType)"
                if !srcCol.isNullable { colDef += " NOT NULL" }
                if let def = srcCol.columnDefault { colDef += " DEFAULT \(def)" }
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ADD COLUMN \(colDef);")
                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) DROP COLUMN \(quoteIdent(name));")
            }
        }

        for (name, tgtCol) in tgtCols where srcCols[name] == nil {
            differences.append("Column \(name): extra in target (not in source)")
            dropColumnSQL.append("ALTER TABLE \(id.qualifiedName) DROP COLUMN \(quoteIdent(name));")
            var colDef = "\(quoteIdent(name)) \(tgtCol.dataType)"
            if !tgtCol.isNullable { colDef += " NOT NULL" }
            if let def = tgtCol.columnDefault { colDef += " DEFAULT \(def)" }
            reverseDropColumnSQL.append("ALTER TABLE \(id.qualifiedName) ADD COLUMN \(colDef);")
        }

        // Compare constraints
        let srcConstraintMap = Dictionary(uniqueKeysWithValues: srcMeta.constraints.map { ($0.name, $0) })
        let tgtConstraintMap = Dictionary(uniqueKeysWithValues: tgtMeta.constraints.map { ($0.name, $0) })
        let srcConstraintNames = Set(srcConstraintMap.keys)
        let tgtConstraintNames = Set(tgtConstraintMap.keys)
        for name in srcConstraintNames.subtracting(tgtConstraintNames) {
            if let con = srcConstraintMap[name] {
                differences.append("Constraint \(name): missing in target")
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ADD CONSTRAINT \(quoteIdent(name)) \(con.definition);")
                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) DROP CONSTRAINT \(quoteIdent(name));")
            }
        }
        for name in tgtConstraintNames.subtracting(srcConstraintNames) {
            differences.append("Constraint \(name): extra in target")
            dropColumnSQL.append("ALTER TABLE \(id.qualifiedName) DROP CONSTRAINT \(quoteIdent(name));")
            if let con = tgtConstraintMap[name] {
                reverseDropColumnSQL.append("ALTER TABLE \(id.qualifiedName) ADD CONSTRAINT \(quoteIdent(name)) \(con.definition);")
            }
        }
        // Compare definitions for constraints with the same name
        for name in srcConstraintNames.intersection(tgtConstraintNames) {
            if let srcCon = srcConstraintMap[name], let tgtCon = tgtConstraintMap[name],
               srcCon.definition != tgtCon.definition {
                differences.append("Constraint \(name): definition differs")
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) DROP CONSTRAINT \(quoteIdent(name));")
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ADD CONSTRAINT \(quoteIdent(name)) \(srcCon.definition);")
                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) DROP CONSTRAINT \(quoteIdent(name));")
                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) ADD CONSTRAINT \(quoteIdent(name)) \(tgtCon.definition);")
            }
        }

        // Compare indexes
        let srcIndexMap = Dictionary(uniqueKeysWithValues: srcMeta.indexes.map { ($0.name, $0) })
        let tgtIndexMap = Dictionary(uniqueKeysWithValues: tgtMeta.indexes.map { ($0.name, $0) })
        let srcIndexNames = Set(srcIndexMap.keys)
        let tgtIndexNames = Set(tgtIndexMap.keys)
        for name in srcIndexNames.subtracting(tgtIndexNames) {
            if let idx = srcIndexMap[name] {
                differences.append("Index \(name): missing in target")
                migrationSQL.append("\(idx.definition);")
                reverseMigrationSQL.append("DROP INDEX \(id.schema.map { quoteIdent($0) + "." } ?? "")\(quoteIdent(name));")
            }
        }
        for name in tgtIndexNames.subtracting(srcIndexNames) {
            differences.append("Index \(name): extra in target")
            dropColumnSQL.append("DROP INDEX \(id.schema.map { quoteIdent($0) + "." } ?? "")\(quoteIdent(name));")
            if let idx = tgtIndexMap[name] {
                reverseDropColumnSQL.append("\(idx.definition);")
            }
        }
        // Compare definitions for indexes with the same name
        for name in srcIndexNames.intersection(tgtIndexNames) {
            if let srcIdx = srcIndexMap[name], let tgtIdx = tgtIndexMap[name],
               srcIdx.definition != tgtIdx.definition {
                differences.append("Index \(name): definition differs")
                migrationSQL.append("DROP INDEX \(id.schema.map { quoteIdent($0) + "." } ?? "")\(quoteIdent(name));")
                migrationSQL.append("\(srcIdx.definition);")
                reverseMigrationSQL.append("DROP INDEX \(id.schema.map { quoteIdent($0) + "." } ?? "")\(quoteIdent(name));")
                reverseMigrationSQL.append("\(tgtIdx.definition);")
            }
        }

        // Compare triggers
        let srcTriggers = Dictionary(uniqueKeysWithValues: srcMeta.triggers.map { ($0.name, $0) })
        let tgtTriggers = Dictionary(uniqueKeysWithValues: tgtMeta.triggers.map { ($0.name, $0) })

        for (name, srcTrigger) in srcTriggers {
            if let tgtTrigger = tgtTriggers[name] {
                if srcTrigger.definition != tgtTrigger.definition {
                    differences.append("Trigger \(name): definition differs")
                    migrationSQL.append("DROP TRIGGER \(quoteIdent(name)) ON \(id.qualifiedName);")
                    migrationSQL.append("\(srcTrigger.definition);")
                    reverseMigrationSQL.append("DROP TRIGGER \(quoteIdent(name)) ON \(id.qualifiedName);")
                    reverseMigrationSQL.append("\(tgtTrigger.definition);")
                }
            } else {
                differences.append("Trigger \(name): missing in target")
                migrationSQL.append("\(srcTrigger.definition);")
                reverseMigrationSQL.append("DROP TRIGGER \(quoteIdent(name)) ON \(id.qualifiedName);")
            }
        }
        for (name, tgtTrigger) in tgtTriggers where srcTriggers[name] == nil {
            differences.append("Trigger \(name): extra in target")
            dropColumnSQL.append("DROP TRIGGER \(quoteIdent(name)) ON \(id.qualifiedName);")
            reverseDropColumnSQL.append("\(tgtTrigger.definition);")
        }

        // Compare RLS policies
        let srcRLS = try await source.rlsPolicies(for: id)
        let tgtRLS = try await target.rlsPolicies(for: id)

        if srcRLS.isEnabled != tgtRLS.isEnabled {
            if srcRLS.isEnabled {
                differences.append("RLS: not enabled on target")
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) ENABLE ROW LEVEL SECURITY;")
                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) DISABLE ROW LEVEL SECURITY;")
            } else {
                differences.append("RLS: enabled on target but not on source")
                dropColumnSQL.append("ALTER TABLE \(id.qualifiedName) DISABLE ROW LEVEL SECURITY;")
                reverseDropColumnSQL.append("ALTER TABLE \(id.qualifiedName) ENABLE ROW LEVEL SECURITY;")
            }
        }

        if srcRLS.isForced != tgtRLS.isForced {
            if srcRLS.isForced {
                differences.append("RLS: not forced on target")
                migrationSQL.append("ALTER TABLE \(id.qualifiedName) FORCE ROW LEVEL SECURITY;")
                reverseMigrationSQL.append("ALTER TABLE \(id.qualifiedName) NO FORCE ROW LEVEL SECURITY;")
            } else {
                differences.append("RLS: forced on target but not on source")
                dropColumnSQL.append("ALTER TABLE \(id.qualifiedName) NO FORCE ROW LEVEL SECURITY;")
                reverseDropColumnSQL.append("ALTER TABLE \(id.qualifiedName) FORCE ROW LEVEL SECURITY;")
            }
        }

        let srcPolicies = Dictionary(uniqueKeysWithValues: srcRLS.policies.map { ($0.name, $0) })
        let tgtPolicies = Dictionary(uniqueKeysWithValues: tgtRLS.policies.map { ($0.name, $0) })

        for (name, srcPolicy) in srcPolicies {
            if let tgtPolicy = tgtPolicies[name] {
                if srcPolicy.definition != tgtPolicy.definition {
                    differences.append("RLS policy \(name): definition differs")
                    migrationSQL.append("DROP POLICY \(quoteIdent(name)) ON \(id.qualifiedName);")
                    migrationSQL.append("\(srcPolicy.definition);")
                    reverseMigrationSQL.append("DROP POLICY \(quoteIdent(name)) ON \(id.qualifiedName);")
                    reverseMigrationSQL.append("\(tgtPolicy.definition);")
                }
            } else {
                differences.append("RLS policy \(name): missing in target")
                migrationSQL.append("\(srcPolicy.definition);")
                reverseMigrationSQL.append("DROP POLICY \(quoteIdent(name)) ON \(id.qualifiedName);")
            }
        }
        for (name, tgtPolicy) in tgtPolicies where srcPolicies[name] == nil {
            differences.append("RLS policy \(name): extra in target")
            dropColumnSQL.append("DROP POLICY \(quoteIdent(name)) ON \(id.qualifiedName);")
            reverseDropColumnSQL.append("\(tgtPolicy.definition);")
        }

        guard !differences.isEmpty else { return nil }

        return ObjectDiff(
            id: id,
            differences: differences,
            migrationSQL: migrationSQL,
            dropColumnSQL: dropColumnSQL,
            reverseMigrationSQL: reverseMigrationSQL,
            reverseDropColumnSQL: reverseDropColumnSQL
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
            migrationSQL: ["CREATE OR REPLACE \(keyword) \(id.qualifiedName) AS\n\(srcDef);"],
            reverseMigrationSQL: ["CREATE OR REPLACE \(keyword) \(id.qualifiedName) AS\n\(tgtDef);"]
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
        var reverseAlterParts: [String] = []

        if srcMeta.increment != tgtMeta.increment {
            diffs.append("INCREMENT: \(tgtMeta.increment) -> \(srcMeta.increment)")
            alterParts.append("INCREMENT BY \(srcMeta.increment)")
            reverseAlterParts.append("INCREMENT BY \(tgtMeta.increment)")
        }
        if srcMeta.minValue != tgtMeta.minValue {
            diffs.append("MIN: \(tgtMeta.minValue) -> \(srcMeta.minValue)")
            alterParts.append("MINVALUE \(srcMeta.minValue)")
            reverseAlterParts.append("MINVALUE \(tgtMeta.minValue)")
        }
        if srcMeta.maxValue != tgtMeta.maxValue {
            diffs.append("MAX: \(tgtMeta.maxValue) -> \(srcMeta.maxValue)")
            alterParts.append("MAXVALUE \(srcMeta.maxValue)")
            reverseAlterParts.append("MAXVALUE \(tgtMeta.maxValue)")
        }
        if srcMeta.cacheSize != tgtMeta.cacheSize {
            diffs.append("CACHE: \(tgtMeta.cacheSize) -> \(srcMeta.cacheSize)")
            alterParts.append("CACHE \(srcMeta.cacheSize)")
            reverseAlterParts.append("CACHE \(tgtMeta.cacheSize)")
        }
        if srcMeta.isCycled != tgtMeta.isCycled {
            diffs.append("CYCLE: \(tgtMeta.isCycled ? "YES" : "NO") -> \(srcMeta.isCycled ? "YES" : "NO")")
            alterParts.append(srcMeta.isCycled ? "CYCLE" : "NO CYCLE")
            reverseAlterParts.append(tgtMeta.isCycled ? "CYCLE" : "NO CYCLE")
        }

        if !alterParts.isEmpty {
            sql.append("ALTER SEQUENCE \(id.qualifiedName) \(alterParts.joined(separator: " "));")
        }

        var reverseSQL: [String] = []
        if !reverseAlterParts.isEmpty {
            reverseSQL.append("ALTER SEQUENCE \(id.qualifiedName) \(reverseAlterParts.joined(separator: " "));")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql, reverseMigrationSQL: reverseSQL)
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
        var irreversible: [String] = []

        let missingInTarget = srcLabels.filter { !tgtLabels.contains($0) }
        let extraInTarget = tgtLabels.filter { !srcLabels.contains($0) }

        for label in missingInTarget {
            diffs.append("Label '\(label)': missing in target")
            if let idx = srcLabels.firstIndex(of: label), idx > 0 {
                let before = srcLabels[idx - 1]
                sql.append("ALTER TYPE \(id.qualifiedName) ADD VALUE '\(escapeSQLString(label))' AFTER '\(escapeSQLString(before))';")
            } else {
                sql.append("ALTER TYPE \(id.qualifiedName) ADD VALUE '\(escapeSQLString(label))';")
            }
            irreversible.append("Cannot remove enum value '\(label)' from \(id.qualifiedName) (PostgreSQL limitation)")
        }
        for label in extraInTarget {
            diffs.append("Label '\(label)': extra in target (cannot remove enum values in PostgreSQL)")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql, irreversibleChanges: irreversible)
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
            migrationSQL: [srcDef + ";"],
            reverseMigrationSQL: [tgtDef + ";"]
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
        var reverseSQL: [String] = []

        for (name, srcAttr) in srcAttrs {
            if let tgtAttr = tgtAttrs[name] {
                if srcAttr.dataType != tgtAttr.dataType {
                    diffs.append("Attribute \(name): type \(tgtAttr.dataType) -> \(srcAttr.dataType)")
                    sql.append("ALTER TYPE \(id.qualifiedName) ALTER ATTRIBUTE \(quoteIdent(name)) TYPE \(srcAttr.dataType);")
                    reverseSQL.append("ALTER TYPE \(id.qualifiedName) ALTER ATTRIBUTE \(quoteIdent(name)) TYPE \(tgtAttr.dataType);")
                }
            } else {
                diffs.append("Attribute \(name): missing in target (type: \(srcAttr.dataType))")
                sql.append("ALTER TYPE \(id.qualifiedName) ADD ATTRIBUTE \(quoteIdent(name)) \(srcAttr.dataType);")
                reverseSQL.append("ALTER TYPE \(id.qualifiedName) DROP ATTRIBUTE \(quoteIdent(name));")
            }
        }

        for (name, tgtAttr) in tgtAttrs where srcAttrs[name] == nil {
            diffs.append("Attribute \(name): extra in target")
            sql.append("ALTER TYPE \(id.qualifiedName) DROP ATTRIBUTE \(quoteIdent(name));")
            reverseSQL.append("ALTER TYPE \(id.qualifiedName) ADD ATTRIBUTE \(quoteIdent(name)) \(tgtAttr.dataType);")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql, reverseMigrationSQL: reverseSQL)
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
            migrationSQL: ["ALTER SCHEMA \(id.qualifiedName) OWNER TO \(quoteIdent(srcMeta.owner));"],
            reverseMigrationSQL: ["ALTER SCHEMA \(id.qualifiedName) OWNER TO \(quoteIdent(tgtMeta.owner));"]
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
        var reverseAlterParts: [String] = []

        if srcMeta.canLogin != tgtMeta.canLogin {
            diffs.append("LOGIN: \(tgtMeta.canLogin) -> \(srcMeta.canLogin)")
            alterParts.append(srcMeta.canLogin ? "LOGIN" : "NOLOGIN")
            reverseAlterParts.append(tgtMeta.canLogin ? "LOGIN" : "NOLOGIN")
        }
        if srcMeta.isSuperuser != tgtMeta.isSuperuser {
            diffs.append("SUPERUSER: \(tgtMeta.isSuperuser) -> \(srcMeta.isSuperuser)")
            alterParts.append(srcMeta.isSuperuser ? "SUPERUSER" : "NOSUPERUSER")
            reverseAlterParts.append(tgtMeta.isSuperuser ? "SUPERUSER" : "NOSUPERUSER")
        }
        if srcMeta.canCreateDB != tgtMeta.canCreateDB {
            diffs.append("CREATEDB: \(tgtMeta.canCreateDB) -> \(srcMeta.canCreateDB)")
            alterParts.append(srcMeta.canCreateDB ? "CREATEDB" : "NOCREATEDB")
            reverseAlterParts.append(tgtMeta.canCreateDB ? "CREATEDB" : "NOCREATEDB")
        }
        if srcMeta.canCreateRole != tgtMeta.canCreateRole {
            diffs.append("CREATEROLE: \(tgtMeta.canCreateRole) -> \(srcMeta.canCreateRole)")
            alterParts.append(srcMeta.canCreateRole ? "CREATEROLE" : "NOCREATEROLE")
            reverseAlterParts.append(tgtMeta.canCreateRole ? "CREATEROLE" : "NOCREATEROLE")
        }
        if srcMeta.connectionLimit != tgtMeta.connectionLimit {
            diffs.append("CONNECTION LIMIT: \(tgtMeta.connectionLimit) -> \(srcMeta.connectionLimit)")
            alterParts.append("CONNECTION LIMIT \(srcMeta.connectionLimit)")
            reverseAlterParts.append("CONNECTION LIMIT \(tgtMeta.connectionLimit)")
        }

        var sql: [String] = []
        var reverseSQL: [String] = []
        if !alterParts.isEmpty {
            sql.append("ALTER ROLE \(quoteIdent(id.name)) \(alterParts.joined(separator: " "));")
        }
        if !reverseAlterParts.isEmpty {
            reverseSQL.append("ALTER ROLE \(quoteIdent(id.name)) \(reverseAlterParts.joined(separator: " "));")
        }

        // Membership changes
        let srcMembers = Set(srcMeta.memberOf)
        let tgtMembers = Set(tgtMeta.memberOf)
        for role in srcMembers.subtracting(tgtMembers) {
            diffs.append("Membership: missing GRANT \(role)")
            sql.append("GRANT \(quoteIdent(role)) TO \(quoteIdent(id.name));")
            reverseSQL.append("REVOKE \(quoteIdent(role)) FROM \(quoteIdent(id.name));")
        }
        for role in tgtMembers.subtracting(srcMembers) {
            diffs.append("Membership: extra GRANT \(role)")
            sql.append("REVOKE \(quoteIdent(role)) FROM \(quoteIdent(id.name));")
            reverseSQL.append("GRANT \(quoteIdent(role)) TO \(quoteIdent(id.name));")
        }

        guard !diffs.isEmpty else { return nil }
        return ObjectDiff(id: id, differences: diffs, migrationSQL: sql, reverseMigrationSQL: reverseSQL)
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
            migrationSQL: ["ALTER EXTENSION \(quoteIdent(id.name)) UPDATE TO '\(escapeSQLString(srcMeta.version))';"],
            reverseMigrationSQL: ["ALTER EXTENSION \(quoteIdent(id.name)) UPDATE TO '\(escapeSQLString(tgtMeta.version))';"]
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
    /// When `includeDestructive` is true, DROP COLUMN/CONSTRAINT/INDEX/TRIGGER/POLICY
    /// statements and DROP TABLE/VIEW statements for objects only in target are included.
    public func renderMigrationSQL(includeDestructive: Bool = false) -> String {
        var sql: [String] = []

        sql.append("-- pg-schema-evo migration script")
        sql.append("-- Brings target in sync with source")
        sql.append("BEGIN;")
        sql.append("")

        // Modified objects: safe ALTER statements
        for objDiff in modified {
            sql.append("-- Modify \(objDiff.id)")
            sql.append(contentsOf: objDiff.migrationSQL)
            sql.append("")
        }

        // Modified objects: destructive DROP statements (gated)
        if includeDestructive {
            let destructiveDiffs = modified.filter { !$0.dropColumnSQL.isEmpty }
            if !destructiveDiffs.isEmpty {
                sql.append("-- Destructive changes (drop columns, constraints, indexes, triggers, policies)")
                for objDiff in destructiveDiffs {
                    sql.append("-- Drop extras from \(objDiff.id)")
                    sql.append(contentsOf: objDiff.dropColumnSQL)
                }
                sql.append("")
            }
        } else {
            let destructiveDiffs = modified.filter { !$0.dropColumnSQL.isEmpty }
            if !destructiveDiffs.isEmpty {
                sql.append("-- Destructive changes SKIPPED (use --allow-drop-columns to include):")
                for objDiff in destructiveDiffs {
                    for stmt in objDiff.dropColumnSQL {
                        sql.append("-- \(stmt)")
                    }
                }
                sql.append("")
            }
        }

        // Objects only in source — need to be created
        if !onlyInSource.isEmpty {
            sql.append("-- Objects missing in target (use 'clone' or 'sync' command to create):")
            for id in onlyInSource {
                sql.append("-- CREATE \(id)")
            }
            sql.append("")
        }

        // Objects only in target — optionally drop
        if !onlyInTarget.isEmpty {
            if includeDestructive {
                sql.append("-- Drop objects only in target")
                for id in onlyInTarget {
                    sql.append("\(dropStatementFor(id))")
                }
            } else {
                sql.append("-- Objects only in target (use --allow-drop-tables to drop):")
                for id in onlyInTarget {
                    sql.append("-- \(dropStatementFor(id))")
                }
            }
            sql.append("")
        }

        sql.append("COMMIT;")

        return sql.joined(separator: "\n")
    }

    /// Generate a DROP statement for an object identifier.
    private func dropStatementFor(_ id: ObjectIdentifier) -> String {
        let keyword: String
        switch id.type {
        case .table: keyword = "TABLE"
        case .view: keyword = "VIEW"
        case .materializedView: keyword = "MATERIALIZED VIEW"
        case .sequence: keyword = "SEQUENCE"
        case .function: keyword = "FUNCTION"
        case .procedure: keyword = "PROCEDURE"
        case .enum, .compositeType: keyword = "TYPE"
        case .schema: keyword = "SCHEMA"
        case .extension: keyword = "EXTENSION"
        default: keyword = "TABLE" // fallback
        }
        let name = id.type == .role ? id.name : id.qualifiedName
        return "DROP \(keyword) IF EXISTS \(name) CASCADE;"
    }
}

public struct ObjectDiff: Sendable {
    public let id: ObjectIdentifier
    public let differences: [String]
    public let migrationSQL: [String]
    /// Destructive SQL that drops columns, constraints, or indexes from the target.
    /// These are separated so that the SyncOrchestrator can gate them behind safety flags.
    public let dropColumnSQL: [String]
    /// Reverse of migrationSQL — undoes the safe changes.
    public let reverseMigrationSQL: [String]
    /// Reverse of dropColumnSQL — re-adds dropped columns/constraints/indexes.
    public let reverseDropColumnSQL: [String]
    /// Changes that cannot be reversed (e.g. enum value additions).
    public let irreversibleChanges: [String]

    public init(
        id: ObjectIdentifier,
        differences: [String],
        migrationSQL: [String],
        dropColumnSQL: [String] = [],
        reverseMigrationSQL: [String] = [],
        reverseDropColumnSQL: [String] = [],
        irreversibleChanges: [String] = []
    ) {
        self.id = id
        self.differences = differences
        self.migrationSQL = migrationSQL
        self.dropColumnSQL = dropColumnSQL
        self.reverseMigrationSQL = reverseMigrationSQL
        self.reverseDropColumnSQL = reverseDropColumnSQL
        self.irreversibleChanges = irreversibleChanges
    }
}
