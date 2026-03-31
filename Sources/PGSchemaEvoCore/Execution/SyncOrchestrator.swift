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
            if job.syncAll {
                // Full diff: compare all objects of the requested types
                let requestedTypes = Array(Set(job.objects.map(\.id.type)))
                let requestedSchemas = job.objects.compactMap(\.id.schema).first

                let diff = try await differ.diff(
                    source: sourceIntrospector,
                    target: targetIntrospector,
                    schema: requestedSchemas,
                    types: requestedTypes
                )

                // Handle objects only in source (need to be created)
                for id in diff.onlyInSource {
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
                for objDiff in diff.modified {
                    logger.info("Modified object: \(objDiff.id) (\(objDiff.differences.count) difference(s))")
                    let combinedSQL = objDiff.migrationSQL.joined(separator: "\n")
                    if !combinedSQL.isEmpty {
                        steps.append(.alterObject(sql: combinedSQL, id: objDiff.id))
                    }
                }

                // Handle objects only in target (optionally drop)
                if job.dropExtra {
                    for id in diff.onlyInTarget {
                        logger.info("Extra object to drop: \(id)")
                        steps.append(.dropObject(id))
                    }
                }

                logger.info("Sync summary: \(diff.onlyInSource.count) new, \(diff.modified.count) modified, \(diff.matching) matching")
            } else {
                // Targeted diff: only check the specific requested objects
                let requestedIds = Set(job.objects.map(\.id))

                for id in requestedIds {
                    let existsOnSource = await objectExists(id, on: sourceIntrospector)
                    let existsOnTarget = await objectExists(id, on: targetIntrospector)

                    switch (existsOnSource, existsOnTarget) {
                    case (true, false):
                        // Object only in source — create on target
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

                    case (true, true):
                        // Object in both — compare for differences
                        if let objDiff = try await differ.compareObjects(id, source: sourceIntrospector, target: targetIntrospector) {
                            logger.info("Modified object: \(objDiff.id) (\(objDiff.differences.count) difference(s))")
                            let combinedSQL = objDiff.migrationSQL.joined(separator: "\n")
                            if !combinedSQL.isEmpty {
                                steps.append(.alterObject(sql: combinedSQL, id: objDiff.id))
                            }
                        }

                    case (false, true):
                        // Object only in target — drop if requested
                        if job.dropExtra {
                            logger.info("Extra object to drop: \(id)")
                            steps.append(.dropObject(id))
                        }

                    case (false, false):
                        logger.warning("Object \(id) not found in source or target, skipping")
                    }
                }

                let newCount = steps.filter { if case .createObject = $0 { return true }; return false }.count
                let modCount = steps.filter { if case .alterObject = $0 { return true }; return false }.count
                logger.info("Sync summary: \(newCount) new, \(modCount) modified")
            }

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

    /// Check if an object exists in the given database by listing objects and checking membership.
    private func objectExists(_ id: ObjectIdentifier, on introspector: SchemaIntrospector) async -> Bool {
        do {
            let objects = try await introspector.listObjects(schema: id.schema, types: [id.type])
            return objects.contains(id)
        } catch {
            return false
        }
    }
}
