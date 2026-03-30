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
