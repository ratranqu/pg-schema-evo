/// Marker protocol for metadata about a database object, retrieved via introspection.
public protocol ObjectMetadata: Sendable {
    var id: ObjectIdentifier { get }
}

// MARK: - Table Metadata

public struct ColumnInfo: Sendable {
    public let name: String
    public let dataType: String
    public let isNullable: Bool
    public let columnDefault: String?
    public let ordinalPosition: Int
    public let characterMaximumLength: Int?
    public let numericPrecision: Int?
    public let numericScale: Int?
    public let isIdentity: Bool
    public let identityGeneration: String?

    public init(
        name: String,
        dataType: String,
        isNullable: Bool,
        columnDefault: String? = nil,
        ordinalPosition: Int,
        characterMaximumLength: Int? = nil,
        numericPrecision: Int? = nil,
        numericScale: Int? = nil,
        isIdentity: Bool = false,
        identityGeneration: String? = nil
    ) {
        self.name = name
        self.dataType = dataType
        self.isNullable = isNullable
        self.columnDefault = columnDefault
        self.ordinalPosition = ordinalPosition
        self.characterMaximumLength = characterMaximumLength
        self.numericPrecision = numericPrecision
        self.numericScale = numericScale
        self.isIdentity = isIdentity
        self.identityGeneration = identityGeneration
    }
}

public struct ConstraintInfo: Sendable {
    public enum ConstraintType: String, Sendable {
        case primaryKey = "p"
        case foreignKey = "f"
        case unique = "u"
        case check = "c"
        case exclusion = "x"
    }

    public let name: String
    public let type: ConstraintType
    public let definition: String
    /// For foreign keys: the referenced table.
    public let referencedTable: String?

    public init(name: String, type: ConstraintType, definition: String, referencedTable: String? = nil) {
        self.name = name
        self.type = type
        self.definition = definition
        self.referencedTable = referencedTable
    }
}

public struct IndexInfo: Sendable {
    public let name: String
    public let definition: String
    public let isUnique: Bool
    public let isPrimary: Bool

    public init(name: String, definition: String, isUnique: Bool, isPrimary: Bool) {
        self.name = name
        self.definition = definition
        self.isUnique = isUnique
        self.isPrimary = isPrimary
    }
}

public struct TriggerInfo: Sendable {
    public let name: String
    public let definition: String

    public init(name: String, definition: String) {
        self.name = name
        self.definition = definition
    }
}

public struct TableMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let columns: [ColumnInfo]
    public let constraints: [ConstraintInfo]
    public let indexes: [IndexInfo]
    public let triggers: [TriggerInfo]

    public init(
        id: ObjectIdentifier,
        columns: [ColumnInfo],
        constraints: [ConstraintInfo] = [],
        indexes: [IndexInfo] = [],
        triggers: [TriggerInfo] = []
    ) {
        self.id = id
        self.columns = columns
        self.constraints = constraints
        self.indexes = indexes
        self.triggers = triggers
    }
}

// MARK: - View Metadata

public struct ViewMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let definition: String
    public let columns: [ColumnInfo]

    public init(id: ObjectIdentifier, definition: String, columns: [ColumnInfo] = []) {
        self.id = id
        self.definition = definition
        self.columns = columns
    }
}

// MARK: - Materialized View Metadata

public struct MaterializedViewMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let definition: String
    public let columns: [ColumnInfo]
    public let indexes: [IndexInfo]

    public init(id: ObjectIdentifier, definition: String, columns: [ColumnInfo] = [], indexes: [IndexInfo] = []) {
        self.id = id
        self.definition = definition
        self.columns = columns
        self.indexes = indexes
    }
}

// MARK: - Sequence Metadata

public struct SequenceMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let dataType: String
    public let startValue: Int64
    public let increment: Int64
    public let minValue: Int64
    public let maxValue: Int64
    public let cacheSize: Int64
    public let isCycled: Bool
    public let ownedByColumn: String?

    public init(
        id: ObjectIdentifier,
        dataType: String = "bigint",
        startValue: Int64 = 1,
        increment: Int64 = 1,
        minValue: Int64 = 1,
        maxValue: Int64 = Int64.max,
        cacheSize: Int64 = 1,
        isCycled: Bool = false,
        ownedByColumn: String? = nil
    ) {
        self.id = id
        self.dataType = dataType
        self.startValue = startValue
        self.increment = increment
        self.minValue = minValue
        self.maxValue = maxValue
        self.cacheSize = cacheSize
        self.isCycled = isCycled
        self.ownedByColumn = ownedByColumn
    }
}

// MARK: - Enum Metadata

public struct EnumMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let labels: [String]

    public init(id: ObjectIdentifier, labels: [String]) {
        self.id = id
        self.labels = labels
    }
}

// MARK: - Function/Procedure Metadata

public struct FunctionMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let definition: String
    public let language: String
    public let returnType: String?
    public let isStrict: Bool
    public let volatility: String
    public let isSecurityDefiner: Bool
    public let argumentSignature: String

    public init(
        id: ObjectIdentifier,
        definition: String,
        language: String = "sql",
        returnType: String? = nil,
        isStrict: Bool = false,
        volatility: String = "VOLATILE",
        isSecurityDefiner: Bool = false,
        argumentSignature: String = ""
    ) {
        self.id = id
        self.definition = definition
        self.language = language
        self.returnType = returnType
        self.isStrict = isStrict
        self.volatility = volatility
        self.isSecurityDefiner = isSecurityDefiner
        self.argumentSignature = argumentSignature
    }
}

// MARK: - Schema Metadata

public struct SchemaMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let owner: String

    public init(id: ObjectIdentifier, owner: String) {
        self.id = id
        self.owner = owner
    }
}

// MARK: - Role Metadata

public struct RoleMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let canLogin: Bool
    public let isSuperuser: Bool
    public let canCreateDB: Bool
    public let canCreateRole: Bool
    public let connectionLimit: Int
    public let memberOf: [String]

    public init(
        id: ObjectIdentifier,
        canLogin: Bool = false,
        isSuperuser: Bool = false,
        canCreateDB: Bool = false,
        canCreateRole: Bool = false,
        connectionLimit: Int = -1,
        memberOf: [String] = []
    ) {
        self.id = id
        self.canLogin = canLogin
        self.isSuperuser = isSuperuser
        self.canCreateDB = canCreateDB
        self.canCreateRole = canCreateRole
        self.connectionLimit = connectionLimit
        self.memberOf = memberOf
    }
}

// MARK: - Extension Metadata

public struct ExtensionMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let version: String
    public let installedSchema: String?

    public init(id: ObjectIdentifier, version: String, installedSchema: String? = nil) {
        self.id = id
        self.version = version
        self.installedSchema = installedSchema
    }
}

// MARK: - PgDump-based Metadata (hybrid approach for exotic types)
// Aggregates, operators, FDW, and foreign tables use pg_dump for DDL extraction
// rather than full pg_catalog introspection. See ARCHITECTURE.md for rationale.

public struct PgDumpMetadata: ObjectMetadata {
    public let id: ObjectIdentifier
    public let ddl: String

    public init(id: ObjectIdentifier, ddl: String) {
        self.id = id
        self.ddl = ddl
    }
}

// MARK: - Permission Metadata

public struct PermissionGrant: Sendable {
    public let grantee: String
    public let privilege: String
    public let isGrantable: Bool

    public init(grantee: String, privilege: String, isGrantable: Bool = false) {
        self.grantee = grantee
        self.privilege = privilege
        self.isGrantable = isGrantable
    }
}
