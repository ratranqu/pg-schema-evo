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
        let conflictDetector = ConflictDetector(logger: logger)
        var steps: [CloneStep] = []

        // Determine if conflict resolution is explicitly requested.
        // When no conflict flags are set, use legacy behavior for backward compatibility:
        // safe migrations always apply, destructive changes are skipped with a warning
        // (unless --allow-drop-columns is set).
        let conflictResolutionExplicit = job.conflictResolutionExplicit
            || job.conflictFilePath != nil
            || job.resolveFromPath != nil

        // Determine effective conflict strategy:
        // - Legacy --allow-drop-columns maps to sourceWins+force
        // - No explicit conflict flags → use .skip (legacy: skip destructive with warning)
        // - Explicit conflict flags → use as specified
        let effectiveStrategy: ConflictStrategy
        if job.allowDropColumns && !conflictResolutionExplicit {
            effectiveStrategy = .sourceWins
        } else if !conflictResolutionExplicit {
            // Legacy behavior: skip destructive changes with a warning
            effectiveStrategy = .skip
        } else {
            effectiveStrategy = job.conflictStrategy
        }
        let effectiveForce = job.allowDropColumns || job.force

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

                // Detect conflicts from the diff
                let conflictReport = conflictDetector.detect(from: diff)

                // Handle conflict file output (write and return)
                if let conflictFilePath = job.conflictFilePath, !conflictReport.isEmpty {
                    try ConflictFileIO.writeConflictFile(report: conflictReport, to: conflictFilePath)
                    try? await sourceConn.close()
                    try? await targetConn.close()
                    return "Conflict report written to \(conflictFilePath) (\(conflictReport.count) conflict(s)). Edit the file and re-run with --resolve-from \(conflictFilePath)"
                }

                // Resolve conflicts
                let resolutions = try await resolveConflicts(
                    report: conflictReport,
                    strategy: effectiveStrategy,
                    force: effectiveForce,
                    job: job
                )

                // Build resolved SQL from conflict resolutions
                let resolvedSQL = ConflictResolver.sqlForResolutions(resolutions, report: conflictReport)

                // Log skipped conflicts (legacy warning behavior)
                for resolution in resolutions where resolution.choice == .skip {
                    if let conflict = conflictReport.conflicts.first(where: { $0.id == resolution.conflictId }) {
                        logger.warning("Skipping conflict: \(conflict.description) (use --conflict-strategy source-wins --force to apply)")
                    }
                }

                // Handle objects only in source (need to be created) — not conflicts
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

                // Handle modified objects — safe migration SQL is always applied
                for objDiff in diff.modified {
                    logger.info("Modified object: \(objDiff.id) (\(objDiff.differences.count) difference(s))")
                    let combinedSQL = objDiff.migrationSQL.joined(separator: "\n")
                    if !combinedSQL.isEmpty {
                        steps.append(.alterObject(sql: combinedSQL, id: objDiff.id))
                    }
                }

                // Add resolved conflict SQL (destructive changes that were approved)
                if !resolvedSQL.isEmpty {
                    let combinedResolved = resolvedSQL.joined(separator: "\n")
                    steps.append(.rawSQL(sql: combinedResolved))
                }

                // Handle objects only in target:
                // - If conflict resolution approved dropping them, they're in resolvedSQL
                // - If --drop-extra is set, drop remaining extra objects
                if job.dropExtra {
                    let resolvedDropIds = Set(resolutions
                        .filter { $0.choice == .applySource }
                        .compactMap { res in conflictReport.conflicts.first { $0.id == res.conflictId } }
                        .filter { $0.kind == .objectOnlyInTarget }
                        .map(\.objectIdentifier))

                    for id in diff.onlyInTarget where !resolvedDropIds.contains(id.description) {
                        logger.info("Extra object to drop: \(id)")
                        steps.append(.dropObject(id))
                    }
                }

                logger.info("Sync summary: \(diff.onlyInSource.count) new, \(diff.modified.count) modified, \(diff.matching) matching")
            } else {
                // Targeted diff: only check the specific requested objects
                let requestedIds = Set(job.objects.map(\.id))

                // Collect individual diffs to build a SchemaDiff for conflict detection
                var onlyInSource: [ObjectIdentifier] = []
                var onlyInTarget: [ObjectIdentifier] = []
                var modified: [ObjectDiff] = []
                var matchCount = 0

                for id in requestedIds {
                    let existsOnSource = await objectExists(id, on: sourceIntrospector)
                    let existsOnTarget = await objectExists(id, on: targetIntrospector)

                    switch (existsOnSource, existsOnTarget) {
                    case (true, false):
                        onlyInSource.append(id)
                    case (true, true):
                        if let objDiff = try await differ.compareObjects(id, source: sourceIntrospector, target: targetIntrospector) {
                            modified.append(objDiff)
                        } else {
                            matchCount += 1
                        }
                    case (false, true):
                        onlyInTarget.append(id)
                    case (false, false):
                        logger.warning("Object \(id) not found in source or target, skipping")
                    }
                }

                let diff = SchemaDiff(
                    onlyInSource: onlyInSource,
                    onlyInTarget: onlyInTarget,
                    modified: modified,
                    matching: matchCount
                )

                // Detect and resolve conflicts
                let conflictReport = conflictDetector.detect(from: diff)

                if let conflictFilePath = job.conflictFilePath, !conflictReport.isEmpty {
                    try ConflictFileIO.writeConflictFile(report: conflictReport, to: conflictFilePath)
                    try? await sourceConn.close()
                    try? await targetConn.close()
                    return "Conflict report written to \(conflictFilePath) (\(conflictReport.count) conflict(s)). Edit the file and re-run with --resolve-from \(conflictFilePath)"
                }

                let resolutions = try await resolveConflicts(
                    report: conflictReport,
                    strategy: effectiveStrategy,
                    force: effectiveForce,
                    job: job
                )

                let resolvedSQL = ConflictResolver.sqlForResolutions(resolutions, report: conflictReport)

                // Log skipped conflicts
                for resolution in resolutions where resolution.choice == .skip {
                    if let conflict = conflictReport.conflicts.first(where: { $0.id == resolution.conflictId }) {
                        logger.warning("Skipping conflict: \(conflict.description) (use --conflict-strategy source-wins --force to apply)")
                    }
                }

                // Create objects only in source
                for id in onlyInSource {
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

                // Apply safe migration SQL for modified objects
                for objDiff in modified {
                    logger.info("Modified object: \(objDiff.id) (\(objDiff.differences.count) difference(s))")
                    let combinedSQL = objDiff.migrationSQL.joined(separator: "\n")
                    if !combinedSQL.isEmpty {
                        steps.append(.alterObject(sql: combinedSQL, id: objDiff.id))
                    }
                }

                // Add resolved conflict SQL
                if !resolvedSQL.isEmpty {
                    let combinedResolved = resolvedSQL.joined(separator: "\n")
                    steps.append(.rawSQL(sql: combinedResolved))
                }

                // Drop extra objects if requested (legacy behavior)
                if job.dropExtra {
                    for id in onlyInTarget {
                        logger.info("Extra object to drop: \(id)")
                        steps.append(.dropObject(id))
                    }
                }

                let newCount = onlyInSource.count
                let modCount = modified.count
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

    /// Resolve conflicts using the configured strategy and job settings.
    private func resolveConflicts(
        report: ConflictReport,
        strategy: ConflictStrategy,
        force: Bool,
        job: SyncJob
    ) async throws -> [ConflictResolution] {
        guard !report.isEmpty else { return [] }

        // If --resolve-from is specified, load resolutions from file
        if let resolveFromPath = job.resolveFromPath {
            let fileResolutions = try ConflictFileIO.readResolutions(from: resolveFromPath)
            let fileConflicts = try ConflictFileIO.readConflicts(from: resolveFromPath)
            let (matched, unresolved) = ConflictFileIO.matchResolutions(
                fileResolutions: fileResolutions,
                fileConflicts: fileConflicts,
                report: report
            )
            if !unresolved.isEmpty {
                logger.warning("\(unresolved.count) new conflict(s) not in resolution file")
                // Fail on unresolved conflicts unless strategy handles them
                if strategy == .fail {
                    throw PGSchemaEvoError.conflictsDetected(
                        count: unresolved.count,
                        destructive: unresolved.filter(\.isDestructive).count
                    )
                }
            }
            return matched
        }

        let resolver = ConflictResolver(strategy: strategy, force: force, logger: logger)

        if strategy == .interactive {
            let prompter = TerminalConflictPrompter(autoAccept: job.autoAcceptNonDestructive)
            return try await resolver.resolveInteractive(report: report, prompter: prompter)
        }

        return try resolver.resolve(report: report)
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
