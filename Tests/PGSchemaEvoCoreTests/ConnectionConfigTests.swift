import Testing
@testable import PGSchemaEvoCore

@Suite("ConnectionConfig Tests")
struct ConnectionConfigTests {

    @Test("Parse valid DSN with all components")
    func parseFullDSN() throws {
        let config = try ConnectionConfig.fromDSN("postgresql://myuser:mypass@db.example.com:5433/mydb?sslmode=require")
        #expect(config.host == "db.example.com")
        #expect(config.port == 5433)
        #expect(config.database == "mydb")
        #expect(config.username == "myuser")
        #expect(config.password == "mypass")
        #expect(config.sslMode == .require)
    }

    @Test("Parse DSN without password")
    func parseDSNWithoutPassword() throws {
        let config = try ConnectionConfig.fromDSN("postgresql://deploy@db.internal:5432/staging")
        #expect(config.host == "db.internal")
        #expect(config.port == 5432)
        #expect(config.database == "staging")
        #expect(config.username == "deploy")
        #expect(config.password == nil)
    }

    @Test("Parse DSN with default port")
    func parseDefaultPort() throws {
        let config = try ConnectionConfig.fromDSN("postgresql://user@localhost/testdb")
        #expect(config.host == "localhost")
        #expect(config.port == 5432)
        #expect(config.database == "testdb")
        #expect(config.username == "user")
        #expect(config.password == nil)
        #expect(config.sslMode == .disable)
    }

    @Test("Parse DSN with sslmode query parameter")
    func parseSSLModeFromQuery() throws {
        let requireConfig = try ConnectionConfig.fromDSN("postgresql://user@host/db?sslmode=require")
        #expect(requireConfig.sslMode == .require)

        let verifyFullConfig = try ConnectionConfig.fromDSN("postgresql://user@host/db?sslmode=verify-full")
        #expect(verifyFullConfig.sslMode == .verifyFull)

        let disableConfig = try ConnectionConfig.fromDSN("postgresql://user@host/db?sslmode=disable")
        #expect(disableConfig.sslMode == .disable)

        // Unknown sslmode falls back to .disable
        let unknownConfig = try ConnectionConfig.fromDSN("postgresql://user@host/db?sslmode=bogus")
        #expect(unknownConfig.sslMode == .disable)
    }

    @Test("Parse DSN with postgres:// scheme")
    func parsePostgresScheme() throws {
        let config = try ConnectionConfig.fromDSN("postgres://admin:secret@10.0.0.1:5432/prod")
        #expect(config.host == "10.0.0.1")
        #expect(config.username == "admin")
        #expect(config.password == "secret")
        #expect(config.database == "prod")
    }

    @Test("Round-trip DSN generation")
    func roundTripDSN() throws {
        let config = ConnectionConfig(
            host: "myhost",
            port: 5432,
            database: "mydb",
            username: "user",
            password: "pass"
        )
        let dsn = config.toDSN()
        #expect(dsn == "postgresql://user:pass@myhost:5432/mydb")
    }

    @Test("toDSN roundtrip preserves sslmode")
    func roundTripDSNWithSSL() throws {
        let config = ConnectionConfig(
            host: "myhost",
            port: 5432,
            database: "mydb",
            username: "user",
            password: "pass",
            sslMode: .require
        )
        let dsn = config.toDSN()
        #expect(dsn == "postgresql://user:pass@myhost:5432/mydb?sslmode=require")
    }

    @Test("toDSN omits password segment when password is nil")
    func toDSNWithoutPassword() {
        let config = ConnectionConfig(
            host: "host",
            port: 5432,
            database: "db",
            username: "user"
        )
        let dsn = config.toDSN()
        #expect(dsn == "postgresql://user@host:5432/db")
    }

    @Test("DSN with masked password")
    func maskedPasswordDSN() {
        let config = ConnectionConfig(
            host: "host",
            port: 5432,
            database: "db",
            username: "user",
            password: "secret"
        )
        let dsn = config.toDSN(maskPassword: true)
        #expect(dsn.contains("****"))
        #expect(!dsn.contains("secret"))
        #expect(dsn == "postgresql://user:****@host:5432/db")
    }

    @Test("environment() returns PGPASSWORD when password is set")
    func environmentWithPassword() {
        let config = ConnectionConfig(
            host: "host",
            port: 5432,
            database: "db",
            username: "user",
            password: "s3cret"
        )
        let env = config.environment()
        #expect(env["PGPASSWORD"] == "s3cret")
        #expect(env.count == 1)
    }

    @Test("environment() returns empty dict when no password")
    func environmentWithoutPassword() {
        let config = ConnectionConfig(
            host: "host",
            port: 5432,
            database: "db",
            username: "user"
        )
        let env = config.environment()
        #expect(env.isEmpty)
    }

    @Test("Reject invalid DSN")
    func rejectInvalidDSN() {
        #expect(throws: PGSchemaEvoError.self) {
            try ConnectionConfig.fromDSN("not-a-dsn")
        }
    }

    @Test("Reject DSN with missing database")
    func rejectMissingDatabase() {
        #expect(throws: PGSchemaEvoError.self) {
            try ConnectionConfig.fromDSN("postgresql://user@host/")
        }
    }

    @Test("Reject DSN with non-postgresql scheme")
    func rejectNonPostgresScheme() {
        #expect(throws: PGSchemaEvoError.self) {
            try ConnectionConfig.fromDSN("mysql://user:pass@host:3306/mydb")
        }
        #expect(throws: PGSchemaEvoError.self) {
            try ConnectionConfig.fromDSN("http://user@host/db")
        }
    }
}
