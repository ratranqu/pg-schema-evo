import Foundation

/// All errors produced by pg-schema-evo.
public enum PGSchemaEvoError: Error, LocalizedError, Sendable {
    // Connection
    case connectionFailed(endpoint: String, underlying: String)
    case authenticationFailed(endpoint: String)

    // Introspection
    case objectNotFound(ObjectIdentifier)
    case unsupportedObjectType(ObjectType, reason: String)
    case introspectionFailed(ObjectIdentifier, underlying: String)

    // Dependencies
    case dependencyCycle(participants: [ObjectIdentifier])
    case missingDependency(object: ObjectIdentifier, requires: ObjectIdentifier)

    // SQL Generation
    case sqlGenerationFailed(ObjectIdentifier, reason: String)

    // Data Transfer
    case shellCommandFailed(command: String, exitCode: Int32, stderr: String)
    case dataSizeExceedsLimit(table: ObjectIdentifier, sizeBytes: Int)

    // Config
    case configFileNotFound(path: String)
    case configParseError(path: String, underlying: String)
    case undefinedEnvironmentVariable(name: String)

    // Pre-flight
    case preflightFailed(checks: [String])

    // Data Sync
    case noPrimaryKey(ObjectIdentifier)
    case trackingColumnNotFound(table: ObjectIdentifier, column: String)
    case syncStateFileNotFound(path: String)
    case syncStateCorrupted(path: String, underlying: String)

    // Validation
    case invalidObjectSpec(String)
    case invalidDSN(String)

    public var errorDescription: String? {
        switch self {
        case .connectionFailed(let endpoint, let underlying):
            "Failed to connect to '\(endpoint)': \(underlying)"
        case .authenticationFailed(let endpoint):
            "Authentication failed for '\(endpoint)'"
        case .objectNotFound(let id):
            "Object not found: \(id)"
        case .unsupportedObjectType(let type, let reason):
            "Unsupported object type '\(type.displayName)': \(reason)"
        case .introspectionFailed(let id, let underlying):
            "Failed to introspect \(id): \(underlying)"
        case .dependencyCycle(let participants):
            "Dependency cycle detected: \(participants.map(\.description).joined(separator: " -> "))"
        case .missingDependency(let object, let requires):
            "Object \(object) requires \(requires) which is not included"
        case .sqlGenerationFailed(let id, let reason):
            "Failed to generate SQL for \(id): \(reason)"
        case .shellCommandFailed(let command, let exitCode, let stderr):
            "Command '\(command)' failed with exit code \(exitCode): \(stderr)"
        case .dataSizeExceedsLimit(let table, let sizeBytes):
            "Table \(table) size (\(sizeBytes) bytes) exceeds configured limit"
        case .configFileNotFound(let path):
            "Config file not found: \(path)"
        case .configParseError(let path, let underlying):
            "Failed to parse config file '\(path)': \(underlying)"
        case .undefinedEnvironmentVariable(let name):
            "Undefined environment variable: ${\(name)}"
        case .preflightFailed(let checks):
            "Pre-flight checks failed:\n\(checks.map { "  - \($0)" }.joined(separator: "\n"))"
        case .noPrimaryKey(let id):
            "Table \(id) has no primary key, required for incremental data sync"
        case .trackingColumnNotFound(let table, let column):
            "Tracking column '\(column)' not found in table \(table)"
        case .syncStateFileNotFound(let path):
            "Sync state file not found: \(path). Run 'data-sync init' first."
        case .syncStateCorrupted(let path, let underlying):
            "Failed to parse sync state file '\(path)': \(underlying)"
        case .invalidObjectSpec(let message):
            "Invalid object specifier: \(message)"
        case .invalidDSN(let message):
            "Invalid DSN: \(message)"
        }
    }
}
