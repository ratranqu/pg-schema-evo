import Foundation

/// The user's choice for resolving a single conflict.
public enum ResolutionChoice: String, Sendable, Codable, CaseIterable {
    /// Apply source definition (overwrite target).
    case applySource = "apply-source"
    /// Keep target definition (do nothing for this conflict).
    case keepTarget = "keep-target"
    /// Skip this conflict entirely (do nothing).
    case skip
}

/// A resolved conflict — pairs a conflict ID with the chosen resolution.
public struct ConflictResolution: Sendable, Codable, Equatable {
    /// The ID of the conflict this resolves.
    public let conflictId: UUID
    /// The chosen resolution.
    public let choice: ResolutionChoice
    /// When this resolution was made.
    public let timestamp: Date

    public init(
        conflictId: UUID,
        choice: ResolutionChoice,
        timestamp: Date = Date()
    ) {
        self.conflictId = conflictId
        self.choice = choice
        self.timestamp = timestamp
    }
}
