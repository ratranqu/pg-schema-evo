/// Every PostgreSQL object type the tool supports.
public enum ObjectType: String, Codable, CaseIterable, Sendable {
    case table
    case view
    case materializedView = "matview"
    case sequence
    case `enum`
    case compositeType = "type"
    case function
    case procedure
    case aggregate
    case `operator`
    case schema
    case role
    case `extension`
    case foreignDataWrapper = "fdw"
    case foreignTable = "foreign_table"

    /// Human-readable display name.
    public var displayName: String {
        switch self {
        case .table: "table"
        case .view: "view"
        case .materializedView: "materialized view"
        case .sequence: "sequence"
        case .enum: "enum"
        case .compositeType: "composite type"
        case .function: "function"
        case .procedure: "procedure"
        case .aggregate: "aggregate"
        case .operator: "operator"
        case .schema: "schema"
        case .role: "role"
        case .extension: "extension"
        case .foreignDataWrapper: "foreign data wrapper"
        case .foreignTable: "foreign table"
        }
    }

    /// Whether this object type lives inside a schema.
    public var isSchemaScoped: Bool {
        switch self {
        case .role, .extension, .schema:
            false
        default:
            true
        }
    }

    /// Whether this object type can hold row data.
    public var supportsData: Bool {
        switch self {
        case .table, .materializedView, .foreignTable:
            true
        default:
            false
        }
    }
}

/// Fully-qualified reference to a database object.
public struct ObjectIdentifier: Hashable, Sendable, Codable, CustomStringConvertible {
    public let type: ObjectType
    public let schema: String?
    public let name: String
    /// For overloaded functions/procedures, e.g. "(integer, text)".
    public let signature: String?

    public init(type: ObjectType, schema: String? = nil, name: String, signature: String? = nil) {
        self.type = type
        self.schema = schema
        self.name = name
        self.signature = signature
    }

    public var description: String {
        var result = "\(type.rawValue):"
        if let schema {
            result += "\(schema)."
        }
        result += name
        if let signature {
            result += signature
        }
        return result
    }

    /// The fully-qualified SQL name (schema.name), quoted if needed.
    public var qualifiedName: String {
        if let schema {
            return "\(quoteIdent(schema)).\(quoteIdent(name))"
        }
        return quoteIdent(name)
    }

    private func quoteIdent(_ ident: String) -> String {
        // Only quote if it contains special chars or is a reserved word candidate.
        // For safety, always quote.
        "\"\(ident.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}

/// Specification for what to do when cloning a specific object.
public struct ObjectSpec: Sendable, Codable {
    public let id: ObjectIdentifier
    public var copyPermissions: Bool
    public var copyData: Bool
    public var cascadeDependencies: Bool

    public init(
        id: ObjectIdentifier,
        copyPermissions: Bool = false,
        copyData: Bool = false,
        cascadeDependencies: Bool = false
    ) {
        self.id = id
        self.copyPermissions = copyPermissions
        self.copyData = copyData
        self.cascadeDependencies = cascadeDependencies
    }
}

/// Parse a CLI object specifier like "table:public.users" or "function:public.calculate_total(integer)".
public func parseObjectSpecifier(_ spec: String) throws -> ObjectIdentifier {
    guard let colonIndex = spec.firstIndex(of: ":") else {
        throw PGSchemaEvoError.invalidObjectSpec(
            "Expected format 'type:schema.name' or 'type:name', got '\(spec)'"
        )
    }

    let typeStr = String(spec[spec.startIndex..<colonIndex])
    let rest = String(spec[spec.index(after: colonIndex)...])

    guard let type = ObjectType(rawValue: typeStr) else {
        let validTypes = ObjectType.allCases.map(\.rawValue).joined(separator: ", ")
        throw PGSchemaEvoError.invalidObjectSpec(
            "Unknown object type '\(typeStr)'. Valid types: \(validTypes)"
        )
    }

    // Extract signature for functions/procedures: "schema.name(args)"
    var nameAndSchema = rest
    var signature: String?
    if let parenStart = rest.firstIndex(of: "(") {
        signature = String(rest[parenStart...])
        nameAndSchema = String(rest[rest.startIndex..<parenStart])
    }

    let parts = nameAndSchema.split(separator: ".", maxSplits: 1).map(String.init)
    let schema: String?
    let name: String

    if parts.count == 2 {
        schema = parts[0]
        name = parts[1]
    } else if parts.count == 1 {
        if type.isSchemaScoped {
            // Default to "public" schema for schema-scoped objects
            schema = "public"
        } else {
            schema = nil
        }
        name = parts[0]
    } else {
        throw PGSchemaEvoError.invalidObjectSpec("Empty object name in '\(spec)'")
    }

    return ObjectIdentifier(type: type, schema: schema, name: name, signature: signature)
}
