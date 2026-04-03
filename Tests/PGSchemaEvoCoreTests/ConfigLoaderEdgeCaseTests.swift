import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ConfigLoader Edge Case Tests")
struct ConfigLoaderEdgeCaseTests {
    let loader = ConfigLoader()

    // MARK: - interpolateEnvVars edge cases

    @Test("Multiple env vars in same string")
    func multipleEnvVarsInString() throws {
        let home = ProcessInfo.processInfo.environment["HOME"] ?? ""
        let user = ProcessInfo.processInfo.environment["USER"] ?? ProcessInfo.processInfo.environment["LOGNAME"] ?? ""
        let result = try loader.interpolateEnvVars("path: ${HOME}/data user: ${USER:-${LOGNAME:-unknown}}")
        #expect(result.contains(home))
    }

    @Test("Env var with empty default")
    func envVarEmptyDefault() throws {
        let result = try loader.interpolateEnvVars("val: ${NONEXISTENT_XYZ_ABC:-}")
        #expect(result == "val: ")
    }

    @Test("Env var default with special characters")
    func envVarDefaultSpecialChars() throws {
        let result = try loader.interpolateEnvVars("val: ${NONEXISTENT_XYZ_ABC:-host:5432/db}")
        #expect(result == "val: host:5432/db")
    }

    @Test("No env vars returns unchanged string")
    func noEnvVars() throws {
        let input = "source:\n  host: localhost\n  database: mydb"
        let result = try loader.interpolateEnvVars(input)
        #expect(result == input)
    }

    @Test("Env var at start of string")
    func envVarAtStart() throws {
        let result = try loader.interpolateEnvVars("${NONEXISTENT_XYZ:-hello} world")
        #expect(result == "hello world")
    }

    @Test("Env var at end of string")
    func envVarAtEnd() throws {
        let result = try loader.interpolateEnvVars("hello ${NONEXISTENT_XYZ:-world}")
        #expect(result == "hello world")
    }

    @Test("Multiple real env vars in same string")
    func multipleRealEnvVars() throws {
        // HOME and PATH should both be set in any Unix environment
        let result = try loader.interpolateEnvVars("home=${HOME} path_set=${PATH}")
        #expect(result.contains("home="))
        #expect(result.contains("path_set="))
        #expect(!result.contains("${HOME}"))
        #expect(!result.contains("${PATH}"))
    }

    // MARK: - Connection parsing edge cases

    @Test("Missing host in connection throws error")
    func missingHostThrows() throws {
        let yaml = """
            source:
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    @Test("Missing database in connection throws error")
    func missingDatabaseThrows() throws {
        let yaml = """
            source:
              host: localhost
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    @Test("Connection defaults: port and username")
    func connectionDefaults() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.source.port == 5432)
        #expect(config.source.username == "postgres")
        #expect(config.source.password == nil)
        #expect(config.source.sslMode == .disable)
    }

    @Test("SSL verify-full mode")
    func sslVerifyFull() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              ssl: verify-full
            target:
              host: localhost
              database: devdb
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.source.sslMode == .verifyFull)
    }

    @Test("Invalid SSL mode falls back to disable")
    func invalidSSLModeFallback() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              ssl: not-a-real-mode
            target:
              host: localhost
              database: devdb
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.source.sslMode == .disable)
    }

    // MARK: - Parallel config

    @Test("Parallel value from config file")
    func parallelFromConfig() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            parallel: 4
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.parallel == 4)
    }

    @Test("Parallel override from CLI takes precedence")
    func parallelOverride() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            parallel: 4
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let overrides = ConfigOverrides(parallel: 8)
        let config = try loader.load(path: path, overrides: overrides)
        #expect(config.parallel == 8)
    }

    // MARK: - Data method parsing

    @Test("Data method auto from config")
    func dataMethodAuto() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            defaults:
              data_method: auto
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.defaultDataMethod == .auto)
    }

    @Test("Data method pgdump from config")
    func dataMethodPgDump() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            defaults:
              data_method: pgdump
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.defaultDataMethod == .pgDump)
    }

    @Test("Invalid data method defaults to auto")
    func invalidDataMethodDefault() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            defaults:
              data_method: invalid_method
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.defaultDataMethod == .auto)
    }

    // MARK: - Object spec edge cases

    @Test("Exotic object types in config")
    func exoticObjectTypes() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            objects:
              - type: foreign_table
                schema: public
                name: remote_table
              - type: aggregate
                schema: public
                name: my_agg
                signature: "(integer)"
              - type: fdw
                name: my_fdw
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.objects.count == 3)
        #expect(config.objects[0].id.type == .foreignTable)
        #expect(config.objects[1].id.type == .aggregate)
        #expect(config.objects[1].id.signature == "(integer)")
        #expect(config.objects[2].id.type == .foreignDataWrapper)
        #expect(config.objects[2].id.schema == "public")  // FDW defaults to public in config parser
    }

    @Test("Object with all optional fields")
    func objectWithAllOptionalFields() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            objects:
              - type: table
                schema: myschema
                name: orders
                permissions: true
                data: true
                cascade: true
                where: "created_at > '2024-01-01'"
                row_limit: 10000
                rls: true
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        let obj = config.objects[0]
        #expect(obj.id.schema == "myschema")
        #expect(obj.copyPermissions == true)
        #expect(obj.copyData == true)
        #expect(obj.cascadeDependencies == true)
        #expect(obj.whereClause == "created_at > '2024-01-01'")
        #expect(obj.rowLimit == 10000)
        #expect(obj.copyRLSPolicies == true)
    }

    // MARK: - Invalid YAML

    @Test("Non-mapping YAML root throws error")
    func nonMappingYAMLThrows() throws {
        let yaml = "- item1\n- item2\n- item3"
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    @Test("Config file is not valid YAML throws error")
    func invalidYAMLThrows() throws {
        let content = "{{{{invalid yaml::::"
        let path = try writeTempFile(content)
        #expect(throws: Error.self) {
            try loader.load(path: path)
        }
    }

    // MARK: - Defaults edge cases

    @Test("No defaults section uses hardcoded defaults")
    func noDefaultsSection() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
            target:
              host: localhost
              database: devdb
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        #expect(config.defaultDataMethod == .auto)
        #expect(config.dataSizeThresholdMB == 100)
        #expect(config.dropIfExists == false)
        #expect(config.dryRun == false)
        #expect(config.force == false)
        #expect(config.parallel == 0)
    }

    // MARK: - File read errors

    @Test("Config file that exists but cannot be read throws error")
    func unreadableConfigFile() throws {
        // Create a directory where a file is expected
        let dirPath = NSTemporaryDirectory() + "pg-schema-evo-unreadable-\(UUID().uuidString).yaml"
        try FileManager.default.createDirectory(atPath: dirPath, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(atPath: dirPath) }
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: dirPath)
        }
    }

    // MARK: - Helper

    private func writeTempFile(_ content: String) throws -> String {
        let path = NSTemporaryDirectory() + "pg-schema-evo-test-\(UUID().uuidString).yaml"
        try content.write(toFile: path, atomically: true, encoding: .utf8)
        return path
    }
}
