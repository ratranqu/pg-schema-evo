import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ConfigLoader Tests")
struct ConfigLoaderTests {
    let loader = ConfigLoader()

    @Test("Load minimal config file")
    func loadMinimalConfig() throws {
        let yaml = """
            source:
              host: prod-db.internal
              port: 5432
              database: myapp
              username: readonly_user
              password: secret123
            target:
              host: localhost
              port: 5432
              database: myapp_dev
              username: admin
              password: devpass
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.source.host == "prod-db.internal")
        #expect(config.source.database == "myapp")
        #expect(config.target.host == "localhost")
        #expect(config.target.database == "myapp_dev")
        #expect(config.objects.count == 1)
        #expect(config.objects[0].id.type == .table)
        #expect(config.objects[0].id.name == "users")
    }

    @Test("Load config with DSN format")
    func loadConfigWithDSN() throws {
        let yaml = """
            source:
              dsn: postgresql://user:pass@prod:5432/mydb
            target:
              dsn: postgresql://admin:secret@localhost:5432/devdb
            objects:
              - type: view
                schema: public
                name: active_users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.source.host == "prod")
        #expect(config.source.username == "user")
        #expect(config.target.host == "localhost")
        #expect(config.target.username == "admin")
    }

    @Test("Config defaults section")
    func configDefaults() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            defaults:
              permissions: true
              data: true
              cascade: true
              data_method: copy
              data_threshold_mb: 50
              drop_existing: true
            objects:
              - type: table
                schema: public
                name: users
              - type: table
                schema: public
                name: orders
                data: false
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.objects[0].copyPermissions == true)
        #expect(config.objects[0].copyData == true)
        #expect(config.objects[0].cascadeDependencies == true)
        // Per-object override
        #expect(config.objects[1].copyData == false)
        #expect(config.defaultDataMethod == .copy)
        #expect(config.dataSizeThresholdMB == 50)
        #expect(config.dropIfExists == true)
    }

    @Test("CLI overrides take precedence")
    func cliOverrides() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            defaults:
              data_method: copy
              data_threshold_mb: 50
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let overrides = ConfigOverrides(
            dryRun: true,
            dataMethod: .pgDump,
            dataThresholdMB: 200
        )
        let config = try loader.load(path: path, overrides: overrides)

        #expect(config.dryRun == true)
        #expect(config.defaultDataMethod == .pgDump)
        #expect(config.dataSizeThresholdMB == 200)
    }

    @Test("Environment variable interpolation")
    func envVarInterpolation() throws {
        let result = try loader.interpolateEnvVars("host: ${HOME}")
        #expect(result.contains("/"))  // HOME should resolve to a path
    }

    @Test("Environment variable with default")
    func envVarWithDefault() throws {
        let result = try loader.interpolateEnvVars("val: ${NONEXISTENT_TEST_VAR_XYZ:-fallback}")
        #expect(result == "val: fallback")
    }

    @Test("Undefined env var throws error")
    func undefinedEnvVarThrows() throws {
        #expect(throws: PGSchemaEvoError.self) {
            try loader.interpolateEnvVars("val: ${NONEXISTENT_TEST_VAR_ABC}")
        }
    }

    @Test("Missing source section throws error")
    func missingSourceThrows() throws {
        let yaml = """
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

    @Test("Missing objects section throws error")
    func missingObjectsThrows() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    @Test("Config file not found throws error")
    func fileNotFoundThrows() throws {
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: "/tmp/nonexistent-pg-schema-evo-config.yaml")
        }
    }

    @Test("Invalid object type throws error")
    func invalidObjectTypeThrows() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - type: invalid_type
                schema: public
                name: foo
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    @Test("Schema-scoped objects default to public schema")
    func defaultPublicSchema() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - type: table
                name: users
              - type: role
                name: admin_role
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.objects[0].id.schema == "public")
        #expect(config.objects[1].id.schema == nil)
    }

    @Test("Multiple object types in config")
    func multipleObjectTypes() throws {
        let yaml = """
            source:
              host: db.host
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
              - type: view
                schema: public
                name: active_users
              - type: enum
                schema: public
                name: status
              - type: function
                schema: public
                name: calc_total
                signature: "(integer)"
              - type: type
                schema: public
                name: address
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.objects.count == 5)
        #expect(config.objects[0].id.type == .table)
        #expect(config.objects[1].id.type == .view)
        #expect(config.objects[2].id.type == .enum)
        #expect(config.objects[3].id.type == .function)
        #expect(config.objects[3].id.signature == "(integer)")
        #expect(config.objects[4].id.type == .compositeType)
    }

    @Test("SSL mode parsing")
    func sslMode() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
              ssl: require
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
        let config = try loader.load(path: path)

        #expect(config.source.sslMode == .require)
    }

    @Test("toCloneJob conversion")
    func toCloneJob() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            defaults:
              data_threshold_mb: 200
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)
        let job = config.toCloneJob()

        #expect(job.dataSizeThreshold == 200 * 1024 * 1024)
        #expect(job.objects.count == 1)
    }

    // MARK: - WHERE clause and rowLimit

    @Test("Config with WHERE clause and rowLimit on objects")
    func whereClauseAndRowLimit() throws {
        let yaml = """
            source:
              host: db.host
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
                where: "active = true"
                row_limit: 500
              - type: table
                schema: public
                name: orders
                row_limit: 1000
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.objects[0].whereClause == "active = true")
        #expect(config.objects[0].rowLimit == 500)
        #expect(config.objects[1].whereClause == nil)
        #expect(config.objects[1].rowLimit == 1000)
    }

    // MARK: - RLS enabled

    @Test("Config with RLS enabled on object")
    func rlsEnabled() throws {
        let yaml = """
            source:
              host: db.host
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
                rls: true
              - type: table
                schema: public
                name: orders
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        #expect(config.objects[0].copyRLSPolicies == true)
        #expect(config.objects[1].copyRLSPolicies == false)
    }

    // MARK: - Force override

    @Test("Config with force override via CLI")
    func forceOverride() throws {
        let yaml = """
            source:
              host: db.host
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
        let overrides = ConfigOverrides(force: true)
        let config = try loader.load(path: path, overrides: overrides)

        #expect(config.force == true)
    }

    // MARK: - Missing target section

    @Test("Missing target section throws error")
    func missingTargetThrows() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
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

    // MARK: - Missing object name

    @Test("Missing object name throws error")
    func missingObjectNameThrows() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - type: table
                schema: public
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    // MARK: - Missing object type

    @Test("Missing object type throws error")
    func missingObjectTypeThrows() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects:
              - schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    // MARK: - Empty objects array

    @Test("Empty objects array throws error")
    func emptyObjectsThrows() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            objects: []
            """
        let path = try writeTempFile(yaml)
        #expect(throws: PGSchemaEvoError.self) {
            try loader.load(path: path)
        }
    }

    // MARK: - CloneJobConfig.toCloneJob preserves all fields

    @Test("toCloneJob preserves all fields correctly")
    func toCloneJobPreservesAllFields() throws {
        let yaml = """
            source:
              host: prod-db
              database: mydb
              username: user
              password: secret
            target:
              host: localhost
              database: devdb
              username: admin
              password: devpass
            defaults:
              permissions: true
              data: true
              cascade: true
              data_method: pgdump
              data_threshold_mb: 250
              drop_existing: true
            objects:
              - type: table
                schema: public
                name: users
                where: "id > 100"
                row_limit: 5000
                rls: true
            """
        let path = try writeTempFile(yaml)
        let overrides = ConfigOverrides(dryRun: true, force: true)
        let config = try loader.load(path: path, overrides: overrides)
        let job = config.toCloneJob()

        #expect(job.source.host == "prod-db")
        #expect(job.source.database == "mydb")
        #expect(job.target.host == "localhost")
        #expect(job.target.database == "devdb")
        #expect(job.dryRun == true)
        #expect(job.defaultDataMethod == .pgDump)
        #expect(job.dataSizeThreshold == 250 * 1024 * 1024)
        #expect(job.dropIfExists == true)
        #expect(job.objects.count == 1)
        #expect(job.objects[0].copyPermissions == true)
        #expect(job.objects[0].copyData == true)
        #expect(job.objects[0].cascadeDependencies == true)
        #expect(job.objects[0].whereClause == "id > 100")
        #expect(job.objects[0].rowLimit == 5000)
        #expect(job.objects[0].copyRLSPolicies == true)
    }

    // MARK: - ConfigOverrides with all fields set

    @Test("ConfigOverrides with all fields override config values")
    func allOverridesApplied() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            defaults:
              permissions: false
              data: false
              cascade: false
              data_method: copy
              data_threshold_mb: 50
              drop_existing: false
            objects:
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let overrides = ConfigOverrides(
            dryRun: true,
            data: true,
            permissions: true,
            cascade: true,
            dataMethod: .pgDump,
            dataThresholdMB: 999,
            dropExisting: true,
            force: true
        )
        let config = try loader.load(path: path, overrides: overrides)

        #expect(config.dryRun == true)
        #expect(config.force == true)
        #expect(config.defaultDataMethod == .pgDump)
        #expect(config.dataSizeThresholdMB == 999)
        #expect(config.dropIfExists == true)
        // Object-level defaults from overrides
        #expect(config.objects[0].copyPermissions == true)
        #expect(config.objects[0].copyData == true)
        #expect(config.objects[0].cascadeDependencies == true)
    }

    // MARK: - Non-data-supporting object ignores data default

    @Test("Non-data-supporting object ignores data default")
    func nonDataObjectIgnoresDataDefault() throws {
        let yaml = """
            source:
              host: db.host
              database: mydb
              username: user
            target:
              host: localhost
              database: devdb
              username: admin
            defaults:
              data: true
            objects:
              - type: function
                schema: public
                name: my_func
              - type: table
                schema: public
                name: users
            """
        let path = try writeTempFile(yaml)
        let config = try loader.load(path: path)

        // function does not support data, so copyData should be false
        #expect(config.objects[0].copyData == false)
        // table supports data, so it gets the default
        #expect(config.objects[1].copyData == true)
    }

    // MARK: - Helper

    private func writeTempFile(_ content: String) throws -> String {
        let path = NSTemporaryDirectory() + "pg-schema-evo-test-\(UUID().uuidString).yaml"
        try content.write(toFile: path, atomically: true, encoding: .utf8)
        return path
    }
}
