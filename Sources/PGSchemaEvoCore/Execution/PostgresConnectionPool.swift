import PostgresNIO
import NIOPosix
import Logging
import Synchronization

/// A simple connection pool for PostgresNIO that supports concurrent operations.
///
/// The pool pre-creates a fixed number of connections and hands them out on demand.
/// When all connections are in use, callers suspend until one becomes available.
public final class PostgresConnectionPool: Sendable {
    private let connections: Mutex<[PostgresConnection]>
    private let semaphore: AsyncSemaphore
    private let logger: Logger

    /// Create a pool with `size` connections to the given database.
    public static func create(
        config: ConnectionConfig,
        size: Int,
        logger: Logger
    ) async throws -> PostgresConnectionPool {
        let eventLoopGroup = MultiThreadedEventLoopGroup.singleton

        var conns: [PostgresConnection] = []
        conns.reserveCapacity(size)

        let pgConfig = PostgresConnection.Configuration(
            host: config.host,
            port: config.port,
            username: config.username,
            password: config.password,
            database: config.database,
            tls: .disable
        )

        for i in 0..<size {
            let eventLoop = eventLoopGroup.next()
            let conn = try await PostgresConnection.connect(
                on: eventLoop,
                configuration: pgConfig,
                id: i + 1,
                logger: logger
            ).get()
            conns.append(conn)
        }

        return PostgresConnectionPool(connections: conns, logger: logger)
    }

    private init(connections: [PostgresConnection], logger: Logger) {
        self.connections = Mutex(connections)
        self.semaphore = AsyncSemaphore(count: connections.count)
        self.logger = logger
    }

    /// Borrow a connection, execute the closure, then return it to the pool.
    public func withConnection<T: Sendable>(
        _ body: @Sendable (PostgresConnection) async throws -> T
    ) async throws -> T {
        await semaphore.wait()
        let conn = connections.withLock { conns -> PostgresConnection in
            conns.removeLast()
        }
        do {
            let result = try await body(conn)
            connections.withLock { $0.append(conn) }
            semaphore.signal()
            return result
        } catch {
            connections.withLock { $0.append(conn) }
            semaphore.signal()
            throw error
        }
    }

    /// Close all connections in the pool.
    public func close() async {
        let conns = connections.withLock { conns -> [PostgresConnection] in
            let all = conns
            conns.removeAll()
            return all
        }
        for conn in conns {
            try? await conn.close()
        }
    }

    /// Number of connections in this pool.
    public var size: Int {
        semaphore.totalCount
    }
}

/// A simple async semaphore for limiting concurrency.
public final class AsyncSemaphore: Sendable {
    private let state: Mutex<SemaphoreState>
    public let totalCount: Int

    struct SemaphoreState {
        var count: Int
        var waiters: [CheckedContinuation<Void, Never>]
    }

    public init(count: Int) {
        self.totalCount = count
        self.state = Mutex(SemaphoreState(count: count, waiters: []))
    }

    public func wait() async {
        let shouldSuspend: Bool = state.withLock { s in
            if s.count > 0 {
                s.count -= 1
                return false
            }
            return true
        }

        if shouldSuspend {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                state.withLock { s in
                    if s.count > 0 {
                        s.count -= 1
                        continuation.resume()
                    } else {
                        s.waiters.append(continuation)
                    }
                }
            }
        }
    }

    public func signal() {
        state.withLock { s in
            if let waiter = s.waiters.first {
                s.waiters.removeFirst()
                waiter.resume()
            } else {
                s.count += 1
            }
        }
    }
}
