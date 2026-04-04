import Foundation

/// The kind of schema conflict detected between source and target.
public enum ConflictKind: String, Sendable, Codable, CaseIterable {
    /// Target has columns, indexes, constraints, triggers, or policies not in source.
    case extraInTarget
    /// Both sides have the same object but definitions differ (type change, nullability, etc.).
    case divergedDefinition
    /// An entire object exists only in target and would be dropped.
    case objectOnlyInTarget
    /// Applying source would cause data loss (e.g., DROP COLUMN, type narrowing).
    case destructiveChange
    /// Change cannot be reversed (e.g., enum value removal in PostgreSQL).
    case irreversibleChange
}

/// A single schema conflict between source and target databases.
public struct SchemaConflict: Sendable, Codable, Equatable {
    /// Unique identifier for this conflict.
    public let id: UUID
    /// The database object this conflict relates to.
    public let objectIdentifier: String
    /// The type of conflict.
    public let kind: ConflictKind
    /// Human-readable description of what diverged.
    public let description: String
    /// SQL statements to apply source state (resolve in favor of source).
    public let sourceSQL: [String]
    /// SQL statements to keep target state (or reverse source changes).
    public let targetSQL: [String]
    /// Whether applying source would cause data loss.
    public let isDestructive: Bool
    /// Whether this change cannot be reversed.
    public let isIrreversible: Bool
    /// Additional context (e.g., column name, data type details).
    public let detail: String?

    public init(
        id: UUID = UUID(),
        objectIdentifier: String,
        kind: ConflictKind,
        description: String,
        sourceSQL: [String],
        targetSQL: [String] = [],
        isDestructive: Bool = false,
        isIrreversible: Bool = false,
        detail: String? = nil
    ) {
        self.id = id
        self.objectIdentifier = objectIdentifier
        self.kind = kind
        self.description = description
        self.sourceSQL = sourceSQL
        self.targetSQL = targetSQL
        self.isDestructive = isDestructive
        self.isIrreversible = isIrreversible
        self.detail = detail
    }
}
