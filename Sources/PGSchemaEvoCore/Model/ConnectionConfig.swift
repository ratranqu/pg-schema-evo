import Foundation

/// SSL mode for PostgreSQL connections.
public enum SSLMode: String, Sendable, Codable {
    case disable
    case require
    case verifyFull = "verify-full"
}

/// Parsed PostgreSQL connection parameters.
public struct ConnectionConfig: Sendable {
    public let host: String
    public let port: Int
    public let database: String
    public let username: String
    public let password: String?
    public let sslMode: SSLMode

    public init(
        host: String,
        port: Int = 5432,
        database: String,
        username: String,
        password: String? = nil,
        sslMode: SSLMode = .disable
    ) {
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.sslMode = sslMode
    }

    /// Parse a DSN like "postgresql://user:pass@host:5432/dbname?sslmode=require".
    public static func fromDSN(_ dsn: String) throws -> ConnectionConfig {
        guard let url = URL(string: dsn),
              let scheme = url.scheme,
              (scheme == "postgresql" || scheme == "postgres"),
              let host = url.host, !host.isEmpty,
              let dbname = url.path.split(separator: "/").first.map(String.init),
              !dbname.isEmpty
        else {
            throw PGSchemaEvoError.invalidDSN(
                "Expected format: postgresql://user:pass@host:port/dbname, got '\(dsn)'"
            )
        }

        let username = url.user ?? "postgres"
        let password = url.password
        let port = url.port ?? 5432

        var sslMode: SSLMode = .disable
        if let query = url.query {
            let params = query.split(separator: "&")
            for param in params {
                let kv = param.split(separator: "=", maxSplits: 1)
                if kv.count == 2 && kv[0] == "sslmode" {
                    sslMode = SSLMode(rawValue: String(kv[1])) ?? .disable
                }
            }
        }

        return ConnectionConfig(
            host: host,
            port: port,
            database: dbname,
            username: username,
            password: password,
            sslMode: sslMode
        )
    }

    /// DSN string for use in shell commands. Masks password if requested.
    public func toDSN(maskPassword: Bool = false) -> String {
        var dsn = "postgresql://\(username)"
        if let password {
            dsn += ":\(maskPassword ? "****" : password)"
        }
        dsn += "@\(host):\(port)/\(database)"
        if sslMode != .disable {
            dsn += "?sslmode=\(sslMode.rawValue)"
        }
        return dsn
    }

    /// Arguments for psql CLI invocation.
    public func psqlArgs() -> [String] {
        [toDSN()]
    }

    /// Arguments for pg_dump CLI invocation.
    public func pgDumpArgs() -> [String] {
        [toDSN()]
    }

    /// Environment variables for psql/pg_dump (primarily PGPASSWORD).
    public func environment() -> [String: String] {
        var env: [String: String] = [:]
        if let password {
            env["PGPASSWORD"] = password
        }
        return env
    }
}
