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

    @Test("Parse DSN with postgres:// scheme")
    func parsePostgresScheme() throws {
        let config = try ConnectionConfig.fromDSN("postgres://admin:secret@10.0.0.1:5432/prod")
        #expect(config.host == "10.0.0.1")
        #expect(config.username == "admin")
        #expect(config.password == "secret")
        #expect(config.database == "prod")
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
    }
}
