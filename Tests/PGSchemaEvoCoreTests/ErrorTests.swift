import Testing
@testable import PGSchemaEvoCore

@Suite("PGSchemaEvoError Tests")
struct ErrorTests {

    // MARK: - Connection Errors

    @Test("connectionFailed includes endpoint and underlying message")
    func connectionFailed() {
        let error = PGSchemaEvoError.connectionFailed(
            endpoint: "localhost:5432",
            underlying: "connection refused"
        )
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("localhost:5432"))
        #expect(desc!.contains("connection refused"))
    }

    @Test("authenticationFailed includes endpoint")
    func authenticationFailed() {
        let error = PGSchemaEvoError.authenticationFailed(endpoint: "prod-db:5432")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("prod-db:5432"))
        #expect(desc!.contains("Authentication"))
    }

    // MARK: - Introspection Errors

    @Test("objectNotFound includes object identifier")
    func objectNotFound() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let error = PGSchemaEvoError.objectNotFound(id)
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("users"))
        #expect(desc!.contains("not found"))
    }

    @Test("unsupportedObjectType includes type and reason")
    func unsupportedObjectType() {
        let error = PGSchemaEvoError.unsupportedObjectType(
            .aggregate,
            reason: "aggregates cannot be cloned"
        )
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("aggregate"))
        #expect(desc!.contains("aggregates cannot be cloned"))
    }

    @Test("introspectionFailed includes object and underlying error")
    func introspectionFailed() {
        let id = ObjectIdentifier(type: .function, schema: "analytics", name: "compute")
        let error = PGSchemaEvoError.introspectionFailed(id, underlying: "permission denied")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("compute"))
        #expect(desc!.contains("permission denied"))
    }

    // MARK: - Dependency Errors

    @Test("dependencyCycle lists all participants")
    func dependencyCycle() {
        let a = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let b = ObjectIdentifier(type: .view, schema: "public", name: "order_summary")
        let c = ObjectIdentifier(type: .table, schema: "public", name: "order_items")
        let error = PGSchemaEvoError.dependencyCycle(participants: [a, b, c])
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("orders"))
        #expect(desc!.contains("order_summary"))
        #expect(desc!.contains("order_items"))
        #expect(desc!.contains("->"))
    }

    @Test("missingDependency includes both object and required object")
    func missingDependency() {
        let object = ObjectIdentifier(type: .view, schema: "public", name: "user_view")
        let requires = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let error = PGSchemaEvoError.missingDependency(object: object, requires: requires)
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("user_view"))
        #expect(desc!.contains("users"))
        #expect(desc!.contains("not included"))
    }

    // MARK: - SQL Generation Errors

    @Test("sqlGenerationFailed includes object and reason")
    func sqlGenerationFailed() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let error = PGSchemaEvoError.sqlGenerationFailed(id, reason: "unsupported column type")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("events"))
        #expect(desc!.contains("unsupported column type"))
    }

    // MARK: - Data Transfer Errors

    @Test("shellCommandFailed includes command, exit code, and stderr")
    func shellCommandFailed() {
        let error = PGSchemaEvoError.shellCommandFailed(
            command: "pg_dump",
            exitCode: 1,
            stderr: "no such table"
        )
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("pg_dump"))
        #expect(desc!.contains("1"))
        #expect(desc!.contains("no such table"))
    }

    @Test("dataSizeExceedsLimit includes table and size")
    func dataSizeExceedsLimit() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "logs")
        let error = PGSchemaEvoError.dataSizeExceedsLimit(table: id, sizeBytes: 5_000_000_000)
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("logs"))
        #expect(desc!.contains("5000000000"))
        #expect(desc!.contains("exceeds"))
    }

    // MARK: - Config Errors

    @Test("configFileNotFound includes path")
    func configFileNotFound() {
        let error = PGSchemaEvoError.configFileNotFound(path: "/etc/pg-schema-evo/config.yaml")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("/etc/pg-schema-evo/config.yaml"))
        #expect(desc!.contains("not found"))
    }

    @Test("configParseError includes path and underlying error")
    func configParseError() {
        let error = PGSchemaEvoError.configParseError(
            path: "clone.yaml",
            underlying: "unexpected token at line 5"
        )
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("clone.yaml"))
        #expect(desc!.contains("unexpected token at line 5"))
    }

    @Test("undefinedEnvironmentVariable includes variable name")
    func undefinedEnvironmentVariable() {
        let error = PGSchemaEvoError.undefinedEnvironmentVariable(name: "DATABASE_URL")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("DATABASE_URL"))
    }

    // MARK: - Pre-flight Errors

    @Test("preflightFailed includes all check messages")
    func preflightFailed() {
        let checks = [
            "Target database is not empty",
            "Source version mismatch",
        ]
        let error = PGSchemaEvoError.preflightFailed(checks: checks)
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("Target database is not empty"))
        #expect(desc!.contains("Source version mismatch"))
        #expect(desc!.contains("Pre-flight"))
    }

    // MARK: - Data Sync Errors

    @Test("noPrimaryKey includes table identifier")
    func noPrimaryKey() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let error = PGSchemaEvoError.noPrimaryKey(id)
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("events"))
        #expect(desc!.contains("primary key"))
    }

    @Test("trackingColumnNotFound includes table and column")
    func trackingColumnNotFound() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let error = PGSchemaEvoError.trackingColumnNotFound(table: id, column: "modified_at")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("orders"))
        #expect(desc!.contains("modified_at"))
    }

    @Test("syncStateFileNotFound includes path")
    func syncStateFileNotFound() {
        let error = PGSchemaEvoError.syncStateFileNotFound(path: "/tmp/state.yaml")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("/tmp/state.yaml"))
        #expect(desc!.contains("data-sync init"))
    }

    @Test("syncStateCorrupted includes path and underlying error")
    func syncStateCorrupted() {
        let error = PGSchemaEvoError.syncStateCorrupted(
            path: "state.yaml",
            underlying: "invalid YAML"
        )
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("state.yaml"))
        #expect(desc!.contains("invalid YAML"))
    }

    // MARK: - Validation Errors

    @Test("invalidObjectSpec includes message")
    func invalidObjectSpec() {
        let error = PGSchemaEvoError.invalidObjectSpec("missing colon separator")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("missing colon separator"))
        #expect(desc!.contains("Invalid object specifier"))
    }

    @Test("invalidDSN includes message")
    func invalidDSN() {
        let error = PGSchemaEvoError.invalidDSN("missing host component")
        let desc = error.errorDescription
        #expect(desc != nil)
        #expect(desc!.contains("missing host component"))
        #expect(desc!.contains("Invalid DSN"))
    }
}
