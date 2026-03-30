/// Protocol for generating DDL SQL from object metadata.
public protocol SQLGenerator: Sendable {
    /// The object types this generator handles.
    var supportedTypes: [ObjectType] { get }

    /// Emit CREATE (or CREATE OR REPLACE) DDL.
    func generateCreate(from metadata: any ObjectMetadata) throws -> String

    /// Emit DROP IF EXISTS DDL.
    func generateDrop(for id: ObjectIdentifier) -> String
}
