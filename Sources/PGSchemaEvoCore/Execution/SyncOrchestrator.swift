import PostgresNIO
import Logging

/// Coordinates incremental sync: detects schema changes between source and target
/// databases using SchemaDiffer, then generates and executes only the necessary
/// ALTER/CREATE/DROP steps to bring the target in sync with the source.
public struct SyncOrchestrator: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Execute an incremental sync job. In dry-run mode, returns the rendered script.
    /// In live mode, executes the delta steps against the target.
    public func execute(job: SyncJob) async throws -> String {
        logger.info("Starting incremental sync for \(job.objects.count) object(s)")

        let sourceConn = try await PostgresConnectionHelper.connect(
            config: job.source,
            logger: logger
        )
        let targetConn = try await PostgresConnectionHelper.connect(
            config: job.target,
            logger: logger
        )

        let sourceIntrospector = PGCatalogIntrospector(connection: sourceConn, logger: logger)
        let targetIntrospector = PGCatalogIntrospector(connection: targetConn, logger: logger)
        let pgDumpIntrospector = PgDumpIntrospector(sourceConfig: job.source, logger: logger)

        // SQL generators for creating new objects
        let tableSQLGen = TableSQLGenerator()
        let viewSQLGen = ViewSQLGenerator()
        let seqSQLGen = SequenceSQLGenerator()
        let enumSQLGen = EnumSQLGenerator()
        let funcSQLGen = FunctionSQLGenerator()
        let schemaSQLGen = SchemaSQLGenerator()
        let compositeTypeSQLGen = CompositeTypeSQLGenerator()
        let permissionSQLGen = PermissionSQLGenerator()

        let differ = SchemaDiffer(logger: logger)
        var steps: [CloneStep] = []

        do {
            // Determine which types to diff based on requested objects
            let requestedTypes = Array(Set(job.objects.map(\.id.type)))
            let requestedSchemas = job.objects.compactMap(\.id.schema).first

            // Run the diff
            let diff = try await differ.diff(
                source: sourceIntrospector,
                target: targetIntrospector,
                schema: requestedSchemas,
                types: requestedTypes
            )

            // Filter diff results to only include requested objects
            let requestedIds = Set(job.objects.map(\.id))

            // Handle objects only in source (need to be created)
            let newObjects = job.syncAll
                ? diff.onlyInSource
                : diff.onlyInSource.filter { requestedIds.contains($0) }

            for id in newObjects {
                logger.info("New object to create: \(id)")
                if job.dropIfExists {
                    steps.append(.dropObject(id))
                }
                let createSteps = try await generateCreateSteps(
                    for: id,
                    sourceIntrospector: sourceIntrospector,
                    pgDumpIntrospector: pgDumpIntrospector,
                    tableSQLGen: tableSQLGen,
                    viewSQLGen: viewSQLGen,
                    seqSQLGen: seqSQLGen,
                    enumSQLGen: enumSQLGen,
                    funcSQLGen: funcSQLGen,
                    schemaSQLGen: schemaSQLGen,
                    compositeTypeSQLGen: compositeTypeSQLGen,
                    permissionSQLGen: permissionSQLGen,
                    job: job
                )
                steps.append(contentsOf: createSteps)
            }

            // Handle modified objects (need ALTER statements)
            let modifiedObjects = job.syncAll
                ? diff.modified
                : diff.modified.filter { requestedIds.contains($0.id) }

            for objDiff in modifiedObjects {
                logger.info("Modified object: \(objDiff.id) (\(objDiff.differences.count) difference(s))")
                let combinedSQL = objDiff.migrationSQL.joined(separator: "\n")
                if !combinedSQL.isEmpty {
                    steps.append(.alterObject(sql: combinedSQL, id: objDiff.id))
                }
            }

            // Handle objects only in target (optionally drop)
            if job.dropExtra {
                let extraObjects = job.syncAll
                    ? diff.onlyInTarget
                    : diff.onlyInTarget.filter { requestedIds.contains($0) }

                for id in extraObjects {
                    logger.info("Extra object to drop: \(id)")
                    steps.append(.dropObject(id))
                }
            }

            // Report summary
            let newCount = newObjects.count
            let modCount = modifiedObjects.count
            let matching = diff.matching
            logger.info("Sync summary: \(newCount) new, \(modCount) modified, \(matching) matching")

        } catch {
            try? await sourceConn.close()
            try? await targetConn.close()
            throw error
        }

        try? await sourceConn.close()
        try? await targetConn.close()

        if steps.isEmpty {
            return "Target is already in sync with source. No changes needed."
        }

        if job.dryRun {
            let progress = ProgressReporter(totalSteps: steps.count)
            progress.reportDryRun()
            let renderer = ScriptRenderer()
            let cloneJob = job.toCloneJob()
            return renderer.render(job: cloneJob, steps: steps)
        } else {
            // Confirmation prompt unless --force
            if !job.force {
                let progress = ProgressReporter(totalSteps: steps.count)
                let response = progress.reportConfirmation(
                    targetDSN: job.target.toDSN(maskPassword: true)
                )
                guard response.lowercased() == "yes" else {
                    return "Aborted by user."
                }
            }

            // Install signal handlers for graceful shutdown
            SignalHandler.shared.install()
            defer { SignalHandler.shared.uninstall() }

            let executor = LiveExecutor(logger: logger)
            let cloneJob = job.toCloneJob()
            try await executor.executeInTransaction(steps: steps, job: cloneJob)
            return ""
        }
    }

    /// Generate CREATE steps for a new object (same logic as CloneOrchestrator).
    private func generateCreateSteps(
        for id: ObjectIdentifier,
        sourceIntrospector: SchemaIntrospector,
        pgDumpIntrospector: PgDumpIntrospector,
        tableSQLGen: TableSQLGenerator,
        viewSQLGen: ViewSQLGenerator,
        seqSQLGen: SequenceSQLGenerator,
        enumSQLGen: EnumSQLGenerator,
        funcSQLGen: FunctionSQLGenerator,
        schemaSQLGen: SchemaSQLGenerator,
        compositeTypeSQLGen: CompositeTypeSQLGenerator,
        permissionSQLGen: PermissionSQLGenerator,
        job: SyncJob
    ) async throws -> [CloneStep] {
        var steps: [CloneStep] = []

        switch id.type {
        case .table:
            let metadata = try await sourceIntrospector.describeTable(id)
            let createSQL = try tableSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .view:
            let metadata = try await sourceIntrospector.describeView(id)
            let createSQL = try viewSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .materializedView:
            let metadata = try await sourceIntrospector.describeMaterializedView(id)
            let createSQL = try viewSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .sequence:
            let metadata = try await sourceIntrospector.describeSequence(id)
            let createSQL = try seqSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .enum:
            let metadata = try await sourceIntrospector.describeEnum(id)
            let createSQL = try enumSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .compositeType:
            let metadata = try await sourceIntrospector.describeCompositeType(id)
            let createSQL = try compositeTypeSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .function, .procedure:
            let metadata = try await sourceIntrospector.describeFunction(id)
            let createSQL = try funcSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .schema:
            let metadata = try await sourceIntrospector.describeSchema(id)
            let createSQL = try schemaSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .role:
            let metadata = try await sourceIntrospector.describeRole(id)
            let createSQL = try schemaSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .extension:
            let metadata = try await sourceIntrospector.describeExtension(id)
            let createSQL = try schemaSQLGen.generateCreate(from: metadata)
            steps.append(.createObject(sql: createSQL, id: id))

        case .aggregate, .operator, .foreignDataWrapper, .foreignTable:
            let metadata = try await pgDumpIntrospector.extractDDL(for: id)
            steps.append(.createObject(sql: metadata.ddl, id: id))
        }

        return steps
    }
}
