import PostgresNIO
import NIOPosix
import Logging

/// Helper to create PostgresConnection instances for use in CLI tools.
public enum PostgresConnectionHelper {
    /// Connect to a PostgreSQL server using the given configuration.
    /// Caller is responsible for closing the connection when done.
    public static func connect(
        config: ConnectionConfig,
        logger: Logger
    ) async throws -> PostgresConnection {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton
        let eventLoop = eventLoopGroup.next()

        let pgConfig = PostgresConnection.Configuration(
            host: config.host,
            port: config.port,
            username: config.username,
            password: config.password,
            database: config.database,
            tls: .disable
        )

        return try await PostgresConnection.connect(
            on: eventLoop,
            configuration: pgConfig,
            id: 1,
            logger: logger
        ).get()
    }
}
