import Foundation
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

/// Shared utilities for integration tests.
/// Requires SOURCE_DSN and TARGET_DSN environment variables.
enum IntegrationTestConfig {
    static let sourceDSN = ProcessInfo.processInfo.environment["SOURCE_DSN"]
        ?? "postgresql://testuser:testpass@localhost:15432/source_db"
    static let targetDSN = ProcessInfo.processInfo.environment["TARGET_DSN"]
        ?? "postgresql://testuser:testpass@localhost:15433/target_db"

    static var logger: Logger {
        var logger = Logger(label: "pg-schema-evo-tests")
        logger.logLevel = .debug
        return logger
    }

    static func sourceConfig() throws -> ConnectionConfig {
        try ConnectionConfig.fromDSN(sourceDSN)
    }

    static func targetConfig() throws -> ConnectionConfig {
        try ConnectionConfig.fromDSN(targetDSN)
    }

    static func connect(to config: ConnectionConfig) async throws -> PostgresConnection {
        try await PostgresConnectionHelper.connect(config: config, logger: logger)
    }

    /// Execute raw SQL on a connection, ignoring results.
    static func execute(_ sql: String, on connection: PostgresConnection) async throws {
        _ = try await connection.query(PostgresQuery(unsafeSQL: sql), logger: logger)
    }

    /// Clean up test objects from the target database.
    static func cleanupTarget(connection: PostgresConnection) async throws {
        try await execute("DROP TABLE IF EXISTS public.test_clone_users CASCADE", on: connection)
        try await execute("DROP TABLE IF EXISTS public.users CASCADE", on: connection)
        try await execute("DROP TABLE IF EXISTS public.products CASCADE", on: connection)
    }
}
