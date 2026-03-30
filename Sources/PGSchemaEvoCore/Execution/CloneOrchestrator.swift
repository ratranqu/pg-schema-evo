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
        logger.info("Starting clone operation with \(job.objects.count) object(s)")

        let connection = try await PostgresConnectionHelper.connect(
            config: job.source,
            logger: logger
        )

        let introspector = PGCatalogIntrospector(connection: connection, logger: logger)
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

        var steps: [CloneStep] = []

        do {
            // Resolve dependencies and determine clone order
            let anyCascade = job.objects.contains { $0.cascadeDependencies }
            let orderedSpecs = try await depResolver.resolve(
                objects: job.objects,
                introspector: introspector,
                cascade: anyCascade
            )

            logger.info("Clone order: \(orderedSpecs.map(\.id.description).joined(separator: ", "))")

            for spec in orderedSpecs {
                logger.info("Processing \(spec.id)")

                // Drop if exists
                if job.dropIfExists {
                    steps.append(.dropObject(spec.id))
                }

                // Introspect and generate DDL per object type
                switch spec.id.type {
                case .table:
                    let metadata = try await introspector.describeTable(spec.id)
                    let createSQL = try tableSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                    if spec.copyData {
                        let size = try await introspector.relationSize(spec.id)
                        let method = resolveTransferMethod(
                            preferred: job.defaultDataMethod,
                            size: size,
                            threshold: job.dataSizeThreshold
                        )
                        steps.append(.copyData(id: spec.id, method: method, estimatedSize: size))
                    }

                case .view:
                    let metadata = try await introspector.describeView(spec.id)
                    let createSQL = try viewSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .materializedView:
                    let metadata = try await introspector.describeMaterializedView(spec.id)
                    let createSQL = try viewSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                    if spec.copyData {
                        steps.append(.refreshMaterializedView(spec.id))
                    }

                case .sequence:
                    let metadata = try await introspector.describeSequence(spec.id)
                    let createSQL = try seqSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .enum:
                    let metadata = try await introspector.describeEnum(spec.id)
                    let createSQL = try enumSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .compositeType:
                    let metadata = try await introspector.describeCompositeType(spec.id)
                    let createSQL = try compositeTypeSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .function, .procedure:
                    let metadata = try await introspector.describeFunction(spec.id)
                    let createSQL = try funcSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .schema:
                    let metadata = try await introspector.describeSchema(spec.id)
                    let createSQL = try schemaSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .role:
                    let metadata = try await introspector.describeRole(spec.id)
                    let createSQL = try schemaSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .extension:
                    let metadata = try await introspector.describeExtension(spec.id)
                    let createSQL = try schemaSQLGen.generateCreate(from: metadata)
                    steps.append(.createObject(sql: createSQL, id: spec.id))

                case .aggregate, .operator, .foreignDataWrapper, .foreignTable:
                    // Hybrid approach: use pg_dump for DDL extraction
                    let metadata = try await pgDumpIntrospector.extractDDL(for: spec.id)
                    steps.append(.createObject(sql: metadata.ddl, id: spec.id))

                    // Foreign tables support data copy
                    if spec.copyData && spec.id.type == .foreignTable {
                        let size = try await introspector.relationSize(spec.id)
                        let method = resolveTransferMethod(
                            preferred: job.defaultDataMethod,
                            size: size,
                            threshold: job.dataSizeThreshold
                        )
                        steps.append(.copyData(id: spec.id, method: method, estimatedSize: size))
                    }
                }

                // Permissions
                if spec.copyPermissions {
                    let grants = try await introspector.permissions(for: spec.id)
                    if !grants.isEmpty {
                        let grantSQL = permissionSQLGen.generateGrants(for: spec.id, grants: grants)
                        steps.append(.grantPermissions(sql: grantSQL, id: spec.id))
                    }
                }
            }
        } catch {
            try? await connection.close()
            throw error
        }

        try? await connection.close()

        if job.dryRun {
            let progress = ProgressReporter(totalSteps: steps.count)
            progress.reportDryRun()
            let renderer = ScriptRenderer()
            return renderer.render(job: job, steps: steps)
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

            let executor = LiveExecutor(logger: logger)
            try await executor.execute(steps: steps, job: job)
            return ""
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
