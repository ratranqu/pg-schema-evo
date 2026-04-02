import Testing
@testable import PGSchemaEvoCore

@Suite("Model Tests")
struct ModelTests {

    // MARK: - TransferMethod

    @Test("TransferMethod raw values")
    func transferMethodRawValues() {
        #expect(TransferMethod.copy.rawValue == "copy")
        #expect(TransferMethod.pgDump.rawValue == "pgdump")
        #expect(TransferMethod.auto.rawValue == "auto")
    }

    @Test("TransferMethod from raw value")
    func transferMethodFromRaw() {
        #expect(TransferMethod(rawValue: "copy") == .copy)
        #expect(TransferMethod(rawValue: "pgdump") == .pgDump)
        #expect(TransferMethod(rawValue: "auto") == .auto)
        #expect(TransferMethod(rawValue: "invalid") == nil)
    }

    // MARK: - SSLMode

    @Test("SSLMode raw values")
    func sslModeRawValues() {
        #expect(SSLMode.disable.rawValue == "disable")
        #expect(SSLMode.require.rawValue == "require")
        #expect(SSLMode.verifyFull.rawValue == "verify-full")
    }

    @Test("SSLMode from raw value")
    func sslModeFromRaw() {
        #expect(SSLMode(rawValue: "disable") == .disable)
        #expect(SSLMode(rawValue: "require") == .require)
        #expect(SSLMode(rawValue: "verify-full") == .verifyFull)
        #expect(SSLMode(rawValue: "bogus") == nil)
    }

    // MARK: - ConnectionConfig additional

    @Test("ConnectionConfig default values")
    func connectionConfigDefaults() {
        let config = ConnectionConfig(host: "localhost", database: "mydb", username: "user")
        #expect(config.port == 5432)
        #expect(config.password == nil)
        #expect(config.sslMode == .disable)
    }

    @Test("ConnectionConfig psqlArgs returns DSN")
    func psqlArgs() {
        let config = ConnectionConfig(host: "h", database: "d", username: "u", password: "p")
        let args = config.psqlArgs()
        #expect(args.count == 1)
        #expect(args[0] == "postgresql://u:p@h:5432/d")
    }

    @Test("ConnectionConfig pgDumpArgs returns DSN")
    func pgDumpArgs() {
        let config = ConnectionConfig(host: "h", database: "d", username: "u", password: "p")
        let args = config.pgDumpArgs()
        #expect(args.count == 1)
        #expect(args[0] == "postgresql://u:p@h:5432/d")
    }

    @Test("ConnectionConfig toDSN with verify-full SSL")
    func toDSNVerifyFull() {
        let config = ConnectionConfig(
            host: "secure.host",
            database: "db",
            username: "admin",
            password: "pass",
            sslMode: .verifyFull
        )
        let dsn = config.toDSN()
        #expect(dsn.contains("sslmode=verify-full"))
    }

    @Test("ConnectionConfig toDSN with disable SSL omits sslmode")
    func toDSNDisableSSL() {
        let config = ConnectionConfig(host: "h", database: "d", username: "u", sslMode: .disable)
        let dsn = config.toDSN()
        #expect(!dsn.contains("sslmode"))
    }

    @Test("ConnectionConfig environment with no password returns empty dict")
    func envNoPassword() {
        let config = ConnectionConfig(host: "h", database: "d", username: "u")
        #expect(config.environment().isEmpty)
    }

    @Test("ConnectionConfig environment with password returns PGPASSWORD")
    func envWithPassword() {
        let config = ConnectionConfig(host: "h", database: "d", username: "u", password: "secret")
        let env = config.environment()
        #expect(env["PGPASSWORD"] == "secret")
    }

    // MARK: - ColumnInfo

    @Test("ColumnInfo default values")
    func columnInfoDefaults() {
        let col = ColumnInfo(name: "id", dataType: "integer", isNullable: false, ordinalPosition: 1)
        #expect(col.columnDefault == nil)
        #expect(col.characterMaximumLength == nil)
        #expect(col.numericPrecision == nil)
        #expect(col.numericScale == nil)
        #expect(col.isIdentity == false)
        #expect(col.identityGeneration == nil)
    }

    @Test("ColumnInfo with all values set")
    func columnInfoAllValues() {
        let col = ColumnInfo(
            name: "amount",
            dataType: "numeric",
            isNullable: true,
            columnDefault: "0.0",
            ordinalPosition: 3,
            characterMaximumLength: 255,
            numericPrecision: 10,
            numericScale: 2,
            isIdentity: true,
            identityGeneration: "ALWAYS"
        )
        #expect(col.name == "amount")
        #expect(col.characterMaximumLength == 255)
        #expect(col.numericPrecision == 10)
        #expect(col.numericScale == 2)
        #expect(col.isIdentity == true)
        #expect(col.identityGeneration == "ALWAYS")
    }

    // MARK: - ConstraintInfo

    @Test("ConstraintType raw values")
    func constraintTypeRawValues() {
        #expect(ConstraintInfo.ConstraintType.primaryKey.rawValue == "p")
        #expect(ConstraintInfo.ConstraintType.foreignKey.rawValue == "f")
        #expect(ConstraintInfo.ConstraintType.unique.rawValue == "u")
        #expect(ConstraintInfo.ConstraintType.check.rawValue == "c")
        #expect(ConstraintInfo.ConstraintType.exclusion.rawValue == "x")
    }

    @Test("ConstraintInfo with referenced table")
    func constraintInfoFK() {
        let constraint = ConstraintInfo(
            name: "fk_orders_users",
            type: .foreignKey,
            definition: "FOREIGN KEY (user_id) REFERENCES users(id)",
            referencedTable: "public.users"
        )
        #expect(constraint.referencedTable == "public.users")
    }

    @Test("ConstraintInfo default referencedTable is nil")
    func constraintInfoDefault() {
        let constraint = ConstraintInfo(name: "pk", type: .primaryKey, definition: "PRIMARY KEY (id)")
        #expect(constraint.referencedTable == nil)
    }

    // MARK: - IndexInfo

    @Test("IndexInfo stores all properties")
    func indexInfo() {
        let idx = IndexInfo(
            name: "idx_users_email",
            definition: "CREATE INDEX idx_users_email ON users (email)",
            isUnique: true,
            isPrimary: false
        )
        #expect(idx.name == "idx_users_email")
        #expect(idx.isUnique == true)
        #expect(idx.isPrimary == false)
    }

    // MARK: - TriggerInfo

    @Test("TriggerInfo stores name and definition")
    func triggerInfo() {
        let trg = TriggerInfo(
            name: "trg_audit",
            definition: "CREATE TRIGGER trg_audit AFTER INSERT ON orders FOR EACH ROW EXECUTE FUNCTION audit()"
        )
        #expect(trg.name == "trg_audit")
        #expect(trg.definition.contains("AFTER INSERT"))
    }

    // MARK: - Metadata types

    @Test("TableMetadata with defaults")
    func tableMetadataDefaults() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "t")
        let meta = TableMetadata(id: id, columns: [
            ColumnInfo(name: "id", dataType: "int", isNullable: false, ordinalPosition: 1),
        ])
        #expect(meta.constraints.isEmpty)
        #expect(meta.indexes.isEmpty)
        #expect(meta.triggers.isEmpty)
    }

    @Test("ViewMetadata with defaults")
    func viewMetadataDefaults() {
        let id = ObjectIdentifier(type: .view, schema: "public", name: "v")
        let meta = ViewMetadata(id: id, definition: "SELECT 1")
        #expect(meta.columns.isEmpty)
    }

    @Test("MaterializedViewMetadata with defaults")
    func matviewMetadataDefaults() {
        let id = ObjectIdentifier(type: .materializedView, schema: "public", name: "mv")
        let meta = MaterializedViewMetadata(id: id, definition: "SELECT 1")
        #expect(meta.columns.isEmpty)
        #expect(meta.indexes.isEmpty)
    }

    @Test("SequenceMetadata defaults")
    func sequenceMetadataDefaults() {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "seq")
        let meta = SequenceMetadata(id: id)
        #expect(meta.dataType == "bigint")
        #expect(meta.startValue == 1)
        #expect(meta.increment == 1)
        #expect(meta.minValue == 1)
        #expect(meta.maxValue == Int64.max)
        #expect(meta.cacheSize == 1)
        #expect(meta.isCycled == false)
        #expect(meta.ownedByColumn == nil)
    }

    @Test("SequenceMetadata custom values")
    func sequenceMetadataCustom() {
        let id = ObjectIdentifier(type: .sequence, schema: "public", name: "seq")
        let meta = SequenceMetadata(
            id: id,
            dataType: "integer",
            startValue: 100,
            increment: 5,
            minValue: 10,
            maxValue: 10000,
            cacheSize: 20,
            isCycled: true,
            ownedByColumn: "public.users.id"
        )
        #expect(meta.dataType == "integer")
        #expect(meta.startValue == 100)
        #expect(meta.increment == 5)
        #expect(meta.ownedByColumn == "public.users.id")
    }

    @Test("FunctionMetadata defaults")
    func functionMetadataDefaults() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "f")
        let meta = FunctionMetadata(id: id, definition: "SELECT 1")
        #expect(meta.language == "sql")
        #expect(meta.returnType == nil)
        #expect(meta.isStrict == false)
        #expect(meta.volatility == "VOLATILE")
        #expect(meta.isSecurityDefiner == false)
        #expect(meta.argumentSignature == "")
    }

    @Test("FunctionMetadata custom values")
    func functionMetadataCustom() {
        let id = ObjectIdentifier(type: .function, schema: "public", name: "f")
        let meta = FunctionMetadata(
            id: id,
            definition: "BEGIN END",
            language: "plpgsql",
            returnType: "integer",
            isStrict: true,
            volatility: "IMMUTABLE",
            isSecurityDefiner: true,
            argumentSignature: "(integer, text)"
        )
        #expect(meta.language == "plpgsql")
        #expect(meta.returnType == "integer")
        #expect(meta.isStrict == true)
        #expect(meta.volatility == "IMMUTABLE")
        #expect(meta.isSecurityDefiner == true)
        #expect(meta.argumentSignature == "(integer, text)")
    }

    @Test("RoleMetadata defaults")
    func roleMetadataDefaults() {
        let id = ObjectIdentifier(type: .role, name: "r")
        let meta = RoleMetadata(id: id)
        #expect(meta.canLogin == false)
        #expect(meta.isSuperuser == false)
        #expect(meta.canCreateDB == false)
        #expect(meta.canCreateRole == false)
        #expect(meta.connectionLimit == -1)
        #expect(meta.memberOf.isEmpty)
    }

    @Test("ExtensionMetadata with installed schema")
    func extensionMetadata() {
        let id = ObjectIdentifier(type: .extension, name: "postgis")
        let meta = ExtensionMetadata(id: id, version: "3.4.0", installedSchema: "public")
        #expect(meta.version == "3.4.0")
        #expect(meta.installedSchema == "public")
    }

    @Test("ExtensionMetadata default installed schema is nil")
    func extensionMetadataDefaultSchema() {
        let id = ObjectIdentifier(type: .extension, name: "postgis")
        let meta = ExtensionMetadata(id: id, version: "3.4.0")
        #expect(meta.installedSchema == nil)
    }

    @Test("CompositeTypeAttribute stores all properties")
    func compositeTypeAttribute() {
        let attr = CompositeTypeAttribute(name: "street", dataType: "text", ordinalPosition: 1)
        #expect(attr.name == "street")
        #expect(attr.dataType == "text")
        #expect(attr.ordinalPosition == 1)
    }

    @Test("PgDumpMetadata stores id and ddl")
    func pgDumpMetadata() {
        let id = ObjectIdentifier(type: .aggregate, schema: "public", name: "my_agg")
        let meta = PgDumpMetadata(id: id, ddl: "CREATE AGGREGATE my_agg (sfunc = my_sfunc);")
        #expect(meta.ddl.contains("CREATE AGGREGATE"))
    }

    // MARK: - RLS metadata types

    @Test("RLSPolicy stores name and definition")
    func rlsPolicy() {
        let policy = RLSPolicy(name: "user_access", definition: "CREATE POLICY user_access ON t USING (true)")
        #expect(policy.name == "user_access")
    }

    @Test("RLSInfo defaults")
    func rlsInfoDefaults() {
        let info = RLSInfo()
        #expect(info.isEnabled == false)
        #expect(info.isForced == false)
        #expect(info.policies.isEmpty)
    }

    @Test("RLSInfo with values")
    func rlsInfoCustom() {
        let info = RLSInfo(
            isEnabled: true,
            isForced: true,
            policies: [RLSPolicy(name: "p", definition: "def")]
        )
        #expect(info.isEnabled)
        #expect(info.isForced)
        #expect(info.policies.count == 1)
    }

    // MARK: - Partition metadata

    @Test("PartitionInfo stores strategy and key")
    func partitionInfo() {
        let info = PartitionInfo(strategy: "RANGE", partitionKey: "created_at")
        #expect(info.strategy == "RANGE")
        #expect(info.partitionKey == "created_at")
    }

    @Test("PartitionChild stores id and bound spec")
    func partitionChild() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders_2024_q1")
        let child = PartitionChild(
            id: id,
            boundSpec: "FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')"
        )
        #expect(child.boundSpec.contains("2024-01-01"))
    }

    // MARK: - PermissionGrant

    @Test("PermissionGrant default isGrantable is false")
    func permissionGrantDefault() {
        let grant = PermissionGrant(grantee: "reader", privilege: "SELECT")
        #expect(grant.isGrantable == false)
    }

    @Test("PermissionGrant with isGrantable")
    func permissionGrantGrantable() {
        let grant = PermissionGrant(grantee: "admin", privilege: "ALL", isGrantable: true)
        #expect(grant.isGrantable == true)
    }

    // MARK: - DataSyncJob

    @Test("DataSyncJob defaults")
    func dataSyncJobDefaults() throws {
        let job = DataSyncJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            tables: []
        )
        #expect(job.stateFilePath == ".pg-schema-evo-sync-state.yaml")
        #expect(job.dryRun == false)
        #expect(job.detectDeletes == false)
        #expect(job.force == false)
        #expect(job.retries == 3)
    }

    @Test("DataSyncJob custom values")
    func dataSyncJobCustom() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let tableConfig = DataSyncTableConfig(id: id, trackingColumn: "updated_at")
        let job = DataSyncJob(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            tables: [tableConfig],
            stateFilePath: "/tmp/state.yaml",
            dryRun: true,
            detectDeletes: true,
            force: true,
            retries: 5
        )
        #expect(job.tables.count == 1)
        #expect(job.stateFilePath == "/tmp/state.yaml")
        #expect(job.dryRun == true)
        #expect(job.detectDeletes == true)
        #expect(job.force == true)
        #expect(job.retries == 5)
    }

    @Test("DataSyncTableConfig stores id and tracking column")
    func dataSyncTableConfig() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "events")
        let config = DataSyncTableConfig(id: id, trackingColumn: "event_id")
        #expect(config.trackingColumn == "event_id")
    }

    @Test("DataSyncState initializes with empty tables")
    func dataSyncState() {
        let state = DataSyncState()
        #expect(state.tables.isEmpty)
    }

    @Test("DataSyncState with tables")
    func dataSyncStateWithTables() {
        let state = DataSyncState(tables: [
            "public.orders": DataSyncTableState(column: "id", lastValue: "1000"),
        ])
        #expect(state.tables.count == 1)
        #expect(state.tables["public.orders"]?.column == "id")
        #expect(state.tables["public.orders"]?.lastValue == "1000")
    }

    @Test("DataSyncTableState stores column and lastValue")
    func dataSyncTableState() {
        let state = DataSyncTableState(column: "updated_at", lastValue: "2024-01-15 10:30:00")
        #expect(state.column == "updated_at")
        #expect(state.lastValue == "2024-01-15 10:30:00")
    }

    // MARK: - SyncJob

    @Test("SyncJob allowDropColumns default is false")
    func syncJobAllowDropColumnsDefault() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: []
        )
        #expect(job.allowDropColumns == false)
    }

    @Test("SyncJob toCloneJob preserves skipPreflight")
    func syncJobToCloneJobSkipPreflight() throws {
        let job = SyncJob(
            source: try ConnectionConfig.fromDSN("postgresql://localhost/src"),
            target: try ConnectionConfig.fromDSN("postgresql://localhost/tgt"),
            objects: [],
            skipPreflight: true
        )
        let cloneJob = job.toCloneJob()
        #expect(cloneJob.skipPreflight == true)
    }

    // MARK: - CloneJobConfig

    @Test("CloneJobConfig toCloneJob converts threshold correctly")
    func cloneJobConfigToCloneJob() {
        let config = CloneJobConfig(
            source: ConnectionConfig(host: "h", database: "d", username: "u"),
            target: ConnectionConfig(host: "h", database: "d", username: "u"),
            objects: [],
            dryRun: true,
            defaultDataMethod: .pgDump,
            dataSizeThresholdMB: 50,
            dropIfExists: true,
            force: true,
            parallel: 4
        )
        let job = config.toCloneJob()
        #expect(job.dataSizeThreshold == 50 * 1024 * 1024)
        #expect(job.defaultDataMethod == .pgDump)
        #expect(job.dropIfExists == true)
        #expect(job.parallel == 4)
    }

    // MARK: - ConfigOverrides

    @Test("ConfigOverrides defaults are all nil")
    func configOverridesDefaults() {
        let overrides = ConfigOverrides()
        #expect(overrides.dryRun == nil)
        #expect(overrides.data == nil)
        #expect(overrides.permissions == nil)
        #expect(overrides.cascade == nil)
        #expect(overrides.dataMethod == nil)
        #expect(overrides.dataThresholdMB == nil)
        #expect(overrides.dropExisting == nil)
        #expect(overrides.force == nil)
        #expect(overrides.parallel == nil)
    }

    @Test("ConfigOverrides custom values")
    func configOverridesCustom() {
        let overrides = ConfigOverrides(
            dryRun: true,
            data: true,
            permissions: true,
            cascade: true,
            dataMethod: .copy,
            dataThresholdMB: 200,
            dropExisting: true,
            force: true,
            parallel: 8
        )
        #expect(overrides.dryRun == true)
        #expect(overrides.data == true)
        #expect(overrides.permissions == true)
        #expect(overrides.cascade == true)
        #expect(overrides.dataMethod == .copy)
        #expect(overrides.dataThresholdMB == 200)
        #expect(overrides.dropExisting == true)
        #expect(overrides.force == true)
        #expect(overrides.parallel == 8)
    }

    // MARK: - ShellResult

    @Test("ShellResult succeeded returns true for exit code 0")
    func shellResultSucceeded() {
        let result = ShellResult(exitCode: 0, stdout: "ok", stderr: "")
        #expect(result.succeeded == true)
    }

    @Test("ShellResult succeeded returns false for non-zero exit code")
    func shellResultFailed() {
        let result = ShellResult(exitCode: 1, stdout: "", stderr: "error")
        #expect(result.succeeded == false)
    }

    @Test("ShellResult stores all fields")
    func shellResultFields() {
        let result = ShellResult(exitCode: 42, stdout: "output", stderr: "warning")
        #expect(result.exitCode == 42)
        #expect(result.stdout == "output")
        #expect(result.stderr == "warning")
    }
}
