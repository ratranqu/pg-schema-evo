import PostgresNIO
import Logging

/// Coordinates the entire clone workflow: introspect, resolve dependencies,
/// generate DDL, and either render a dry-run script or execute live via psql.
public struct CloneOrchestrator: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Execute a clone job. In dry-run mode, returns the rendered bash script.
    /// In live mode, executes each step against the target via psql.
    public func execute(job: CloneJob) async throws -> String {
        // Pre-flight checks
        if !job.skipPreflight && !job.dryRun {
            let checker = PreflightChecker(logger: logger)
            let failures = try await checker.check(job: job)
            if !failures.isEmpty {
                throw PGSchemaEvoError.preflightFailed(checks: failures)
            }
        }

        logger.info("Starting clone operation with \(job.objects.count) object(s)")

        // Resolve effective parallelism
        // parallel > 1: use parallel streaming data transfer (outside transaction)
        // parallel <= 1 (0=auto, 1=sequential): use inline data in transaction (proven path)
        let useParallelData = job.parallel > 1
        let effectiveParallel = useParallelData ? job.parallel : 1
        logger.info("Parallelism: \(effectiveParallel) (configured: \(job.parallel == 0 ? "auto" : String(job.parallel)), parallel data: \(useParallelData))")

        // Use connection pool for parallel introspection
        let poolSize = min(effectiveParallel, 4) // Cap pool for introspection
        let pool = try await PostgresConnectionPool.create(
            config: job.source,
            size: poolSize,
            logger: logger
        )
        defer { Task { await pool.close() } }

        let introspector = PooledIntrospector(pool: pool, logger: logger)
        let pgDumpIntrospector = PgDumpIntrospector(sourceConfig: job.source, logger: logger)
        let depResolver = DependencyResolver()

        // SQL generators
        let tableSQLGen = TableSQLGenerator()
        let viewSQLGen = ViewSQLGenerator()
        let seqSQLGen = SequenceSQLGenerator()
        let enumSQLGen = EnumSQLGenerator()
        let funcSQLGen = FunctionSQLGenerator()
        let schemaSQLGen = SchemaSQLGenerator()
        let compositeTypeSQLGen = CompositeTypeSQLGenerator()
        let permissionSQLGen = PermissionSQLGenerator()

        var ddlSteps: [CloneStep] = []
        var dataTransfers: [DataTransferTask] = []

        do {
            // Resolve dependencies and determine clone order
            let anyCascade = job.objects.contains { $0.cascadeDependencies }
            let orderedSpecs = try await depResolver.resolve(
                objects: job.objects,
                introspector: introspector,
                cascade: anyCascade
            )

            // Build dependency map for data transfer scheduling
            let depGraph = buildDataDependencies(specs: orderedSpecs, depResolver: depResolver)

            logger.info("Clone order: \(orderedSpecs.map(\.id.description).joined(separator: ", "))")

            for spec in orderedSpecs {
                logger.info("Processing \(spec.id)")

                // Drop if exists
                if job.dropIfExists {
                    logger.info("Will drop existing \(spec.id) before cloning")
                    ddlSteps.append(.dropObject(spec.id))
                }

                // Introspect and generate DDL per object type
                switch spec.id.type {
                case .table:
                    let metadata = try await introspector.describeTable(spec.id)

                    // Check for partitioned table
                    if let partInfo = try await introspector.partitionInfo(for: spec.id) {
                        // Create parent as partitioned table
                        let createSQL = try tableSQLGen.generateCreate(from: metadata)
                        // Append PARTITION BY clause before the closing ");".
                        // Find the last ");", which terminates the CREATE TABLE statement.
                        let partitionClause = " PARTITION BY \(partInfo.strategy) (\(partInfo.partitionKey))"
                        let partitionedSQL: String
                        if let lastClosing = createSQL.range(of: ");", options: .backwards) {
                            partitionedSQL = createSQL.replacingCharacters(
                                in: lastClosing,
                                with: ")\(partitionClause);"
                            )
                        } else {
                            // Fallback: append to end (shouldn't happen with valid CREATE TABLE)
                            logger.warning("Could not find closing '); in CREATE TABLE for \(spec.id), appending PARTITION BY")
                            partitionedSQL = createSQL.trimmingCharacters(in: .whitespacesAndNewlines)
                                .replacingOccurrences(of: ";$", with: "\(partitionClause);", options: .regularExpression)
                        }
                        ddlSteps.append(.createObject(sql: partitionedSQL, id: spec.id))

                        // Create and attach each child partition
                        let children = try await introspector.listPartitions(for: spec.id)
                        for child in children {
                            if job.dropIfExists {
                                ddlSteps.append(.dropObject(child.id))
                            }
                            let childMeta = try await introspector.describeTable(child.id)
                            let childSQL = try tableSQLGen.generateCreate(from: childMeta)
                            ddlSteps.append(.createObject(sql: childSQL, id: child.id))

                            let attachSQL = "ALTER TABLE \(spec.id.qualifiedName) ATTACH PARTITION \(child.id.qualifiedName) \(child.boundSpec);"
                            ddlSteps.append(.attachPartition(sql: attachSQL, id: child.id))

                            // Queue data transfer for each partition
                            if spec.copyData {
                                let size = try await introspector.relationSize(child.id)
                                let method = resolveTransferMethod(
                                    preferred: job.defaultDataMethod,
                                    size: size,
                                    threshold: job.dataSizeThreshold
                                )
                                let rowLimit = spec.rowLimit ?? job.globalRowLimit
                                if useParallelData {
                                    dataTransfers.append(DataTransferTask(
                                        id: child.id,
                                        method: method,
                                        estimatedSize: size,
                                        whereClause: spec.whereClause,
                                        rowLimit: rowLimit,
                                        dependsOn: [] // Partitions of same table are independent
                                    ))
                                } else {
                                    ddlSteps.append(.copyData(
                                        id: child.id,
                                        method: method,
                                        estimatedSize: size,
                                        whereClause: spec.whereClause,
                                        rowLimit: rowLimit
                                    ))
                                }
                            }
                        }
                    } else {
                        let createSQL = try tableSQLGen.generateCreate(from: metadata)
                        ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                        if spec.copyData {
                            let size = try await introspector.relationSize(spec.id)
                            let method = resolveTransferMethod(
                                preferred: job.defaultDataMethod,
                                size: size,
                                threshold: job.dataSizeThreshold
                            )
                            let rowLimit = spec.rowLimit ?? job.globalRowLimit
                            if useParallelData {
                                dataTransfers.append(DataTransferTask(
                                    id: spec.id,
                                    method: method,
                                    estimatedSize: size,
                                    whereClause: spec.whereClause,
                                    rowLimit: rowLimit,
                                    dependsOn: depGraph[spec.id] ?? []
                                ))
                            } else {
                                ddlSteps.append(.copyData(
                                    id: spec.id,
                                    method: method,
                                    estimatedSize: size,
                                    whereClause: spec.whereClause,
                                    rowLimit: rowLimit
                                ))
                            }
                        }
                    }

                    // RLS policies
                    if spec.copyRLSPolicies {
                        let rlsInfo = try await introspector.rlsPolicies(for: spec.id)
                        if rlsInfo.isEnabled || !rlsInfo.policies.isEmpty {
                            var rlsSQL = ""
                            if rlsInfo.isEnabled {
                                rlsSQL += "ALTER TABLE \(spec.id.qualifiedName) ENABLE ROW LEVEL SECURITY;\n"
                            }
                            if rlsInfo.isForced {
                                rlsSQL += "ALTER TABLE \(spec.id.qualifiedName) FORCE ROW LEVEL SECURITY;\n"
                            }
                            for policy in rlsInfo.policies {
                                rlsSQL += policy.definition + "\n"
                            }
                            if !rlsSQL.isEmpty {
                                ddlSteps.append(.enableRLS(sql: rlsSQL, id: spec.id))
                            }
                        }
                    }

                case .view:
                    let metadata = try await introspector.describeView(spec.id)
                    let createSQL = try viewSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .materializedView:
                    let metadata = try await introspector.describeMaterializedView(spec.id)
                    let createSQL = try viewSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                    if spec.copyData {
                        ddlSteps.append(.refreshMaterializedView(spec.id))
                    }

                case .sequence:
                    let metadata = try await introspector.describeSequence(spec.id)
                    let createSQL = try seqSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .enum:
                    let metadata = try await introspector.describeEnum(spec.id)
                    let createSQL = try enumSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .compositeType:
                    let metadata = try await introspector.describeCompositeType(spec.id)
                    let createSQL = try compositeTypeSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .function, .procedure:
                    let metadata = try await introspector.describeFunction(spec.id)
                    let createSQL = try funcSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .schema:
                    let metadata = try await introspector.describeSchema(spec.id)
                    let createSQL = try schemaSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .role:
                    let metadata = try await introspector.describeRole(spec.id)
                    let createSQL = try schemaSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .extension:
                    let metadata = try await introspector.describeExtension(spec.id)
                    let createSQL = try schemaSQLGen.generateCreate(from: metadata)
                    ddlSteps.append(.createObject(sql: createSQL, id: spec.id))

                case .aggregate, .operator, .foreignDataWrapper, .foreignTable:
                    // Hybrid approach: use pg_dump for DDL extraction
                    let metadata = try await pgDumpIntrospector.extractDDL(for: spec.id)
                    ddlSteps.append(.createObject(sql: metadata.ddl, id: spec.id))

                    // Foreign tables support data copy
                    if spec.copyData && spec.id.type == .foreignTable {
                        let size = try await introspector.relationSize(spec.id)
                        let method = resolveTransferMethod(
                            preferred: job.defaultDataMethod,
                            size: size,
                            threshold: job.dataSizeThreshold
                        )
                        if useParallelData {
                            dataTransfers.append(DataTransferTask(
                                id: spec.id,
                                method: method,
                                estimatedSize: size,
                                dependsOn: depGraph[spec.id] ?? []
                            ))
                        } else {
                            ddlSteps.append(.copyData(
                                id: spec.id,
                                method: method,
                                estimatedSize: size
                            ))
                        }
                    }
                }

                // Permissions
                if spec.copyPermissions {
                    let grants = try await introspector.permissions(for: spec.id)
                    if !grants.isEmpty {
                        let grantSQL = permissionSQLGen.generateGrants(for: spec.id, grants: grants)
                        ddlSteps.append(.grantPermissions(sql: grantSQL, id: spec.id))
                    }
                }
            }
        } catch {
            throw error
        }

        if job.dryRun {
            let progress = ProgressReporter(totalSteps: ddlSteps.count + dataTransfers.count)
            progress.reportDryRun()
            let renderer = ScriptRenderer()
            return renderer.render(job: job, steps: ddlSteps, dataTransfers: dataTransfers)
        } else {
            // Confirmation prompt unless --force
            if !job.force {
                let progress = ProgressReporter(totalSteps: ddlSteps.count + dataTransfers.count)
                let response = progress.reportConfirmation(
                    targetDSN: job.target.toDSN(maskPassword: true)
                )
                guard response.lowercased() == "yes" else {
                    return "Aborted by user."
                }
            }

            // Install signal handlers for graceful shutdown during live execution
            SignalHandler.shared.install()
            defer { SignalHandler.shared.uninstall() }

            // Phase 1: Execute DDL in transaction (sequential)
            let executor = LiveExecutor(logger: logger)
            try await executeWithRetry(executor: executor, steps: ddlSteps, job: job)

            // Phase 2: Parallel streaming data transfer
            if !dataTransfers.isEmpty {
                logger.info("Starting parallel data transfer (\(dataTransfers.count) table(s), concurrency: \(effectiveParallel))")
                let transfer = ParallelDataTransfer(
                    maxConcurrency: effectiveParallel,
                    shell: ShellRunner(),
                    logger: logger
                )
                try await transfer.execute(
                    transfers: dataTransfers,
                    sourceDSN: job.source.toDSN(),
                    targetDSN: job.target.toDSN(),
                    sourceEnv: job.source.environment(),
                    targetEnv: job.target.environment()
                )
            }
            return ""
        }
    }

    /// Build data dependency map: for each table, which other tables it depends on via FK.
    private func buildDataDependencies(
        specs: [ObjectSpec],
        depResolver: DependencyResolver
    ) -> [ObjectIdentifier: Set<ObjectIdentifier>] {
        // The topological order already encodes deps. We just need table-level FK deps
        // for data transfer scheduling. Tables only depend on FK-referenced tables for data.
        // For now, use the ordering position as a simple heuristic.
        var depMap: [ObjectIdentifier: Set<ObjectIdentifier>] = [:]
        let tableIds = specs.filter { $0.id.type == .table }.map(\.id)
        // Tables earlier in topological order are dependencies of later tables
        var seen: Set<ObjectIdentifier> = []
        for id in tableIds {
            depMap[id] = seen
            seen.insert(id)
        }
        return depMap
    }

    /// Execute with transaction wrapping and retry on transient failures.
    private func executeWithRetry(
        executor: LiveExecutor,
        steps: [CloneStep],
        job: CloneJob
    ) async throws {
        guard !steps.isEmpty else { return }

        var lastError: Error?

        for attempt in 0...job.retries {
            if attempt > 0 {
                logger.warning("Retry attempt \(attempt)/\(job.retries)...")
                // Exponential backoff: 2^attempt seconds
                let delayNs = UInt64(pow(2.0, Double(attempt))) * 1_000_000_000
                try await Task.sleep(nanoseconds: delayNs)
            }

            do {
                try await executor.executeInTransaction(steps: steps, job: job)
                return // Success — reset and return
            } catch let error as PGSchemaEvoError {
                lastError = error
                if case .shellCommandFailed(_, let exitCode, _) = error {
                    // Exit code 1 is usually a SQL error (not transient)
                    // Exit code 2 is connection/transient
                    if exitCode == 1 && attempt > 0 {
                        logger.error("Non-transient error, stopping retries")
                        break
                    }
                }
                logger.warning("Attempt \(attempt + 1) failed: \(error.localizedDescription)")
            } catch {
                lastError = error
                logger.warning("Attempt \(attempt + 1) failed: \(error.localizedDescription)")
            }
        }

        if let lastError {
            throw lastError
        }
    }

    private func resolveTransferMethod(
        preferred: TransferMethod,
        size: Int?,
        threshold: Int
    ) -> TransferMethod {
        switch preferred {
        case .copy, .pgDump:
            return preferred
        case .auto:
            guard let size else { return .copy }
            return size >= threshold ? .pgDump : .copy
        }
    }
}

/// Wraps a connection pool to implement the SchemaIntrospector protocol.
///
/// Each introspection call borrows a connection from the pool, enabling
/// concurrent introspection when used with TaskGroup.
final class PooledIntrospector: SchemaIntrospector, @unchecked Sendable {
    private let pool: PostgresConnectionPool
    private let logger: Logger

    init(pool: PostgresConnectionPool, logger: Logger) {
        self.pool = pool
        self.logger = logger
    }

    func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
        guard let schema = id.schema else {
            throw PGSchemaEvoError.invalidObjectSpec("Table requires a schema: \(id)")
        }

        // Run all 4 introspection queries concurrently on separate connections
        async let columns = pool.withConnection { conn in
            let i = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await i.queryColumns(schema: schema, name: id.name)
        }
        async let constraints = pool.withConnection { conn in
            let i = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await i.queryConstraints(schema: schema, name: id.name)
        }
        async let indexes = pool.withConnection { conn in
            let i = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await i.queryIndexes(schema: schema, name: id.name)
        }
        async let triggers = pool.withConnection { conn in
            let i = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await i.queryTriggers(schema: schema, name: id.name)
        }

        let cols = try await columns
        if cols.isEmpty {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        return TableMetadata(
            id: id,
            columns: cols,
            constraints: try await constraints,
            indexes: try await indexes,
            triggers: try await triggers
        )
    }

    func describeView(_ id: ObjectIdentifier) async throws -> ViewMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeView(id)
        }
    }

    func describeMaterializedView(_ id: ObjectIdentifier) async throws -> MaterializedViewMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeMaterializedView(id)
        }
    }

    func describeSequence(_ id: ObjectIdentifier) async throws -> SequenceMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeSequence(id)
        }
    }

    func describeEnum(_ id: ObjectIdentifier) async throws -> EnumMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeEnum(id)
        }
    }

    func describeFunction(_ id: ObjectIdentifier) async throws -> FunctionMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeFunction(id)
        }
    }

    func describeSchema(_ id: ObjectIdentifier) async throws -> SchemaMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeSchema(id)
        }
    }

    func describeRole(_ id: ObjectIdentifier) async throws -> RoleMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeRole(id)
        }
    }

    func describeCompositeType(_ id: ObjectIdentifier) async throws -> CompositeTypeMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeCompositeType(id)
        }
    }

    func describeExtension(_ id: ObjectIdentifier) async throws -> ExtensionMetadata {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.describeExtension(id)
        }
    }

    func relationSize(_ id: ObjectIdentifier) async throws -> Int? {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.relationSize(id)
        }
    }

    func listObjects(schema: String?, types: [ObjectType]?) async throws -> [ObjectIdentifier] {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.listObjects(schema: schema, types: types)
        }
    }

    func permissions(for id: ObjectIdentifier) async throws -> [PermissionGrant] {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.permissions(for: id)
        }
    }

    func dependencies(for id: ObjectIdentifier) async throws -> [ObjectIdentifier] {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.dependencies(for: id)
        }
    }

    func rlsPolicies(for id: ObjectIdentifier) async throws -> RLSInfo {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.rlsPolicies(for: id)
        }
    }

    func partitionInfo(for id: ObjectIdentifier) async throws -> PartitionInfo? {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.partitionInfo(for: id)
        }
    }

    func listPartitions(for id: ObjectIdentifier) async throws -> [PartitionChild] {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.listPartitions(for: id)
        }
    }

    func primaryKeyColumns(for id: ObjectIdentifier) async throws -> [String] {
        try await pool.withConnection { conn in
            let introspector = PGCatalogIntrospector(connection: conn, logger: self.logger)
            return try await introspector.primaryKeyColumns(for: id)
        }
    }
}
