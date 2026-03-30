import PostgresNIO
import Logging

/// Coordinates the entire clone workflow: introspect, plan, render/execute.
public struct CloneOrchestrator: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Execute a clone job. In dry-run mode, returns the rendered bash script.
    /// In live mode (future), executes directly against the target.
    public func execute(job: CloneJob) async throws -> String {
        logger.info("Starting clone operation with \(job.objects.count) object(s)")

        // Connect to source for introspection
        let sourceConfig = PostgresConnection.Configuration(
            host: job.source.host,
            port: job.source.port,
            username: job.source.username,
            password: job.source.password,
            database: job.source.database,
            tls: .disable
        )

        let connection = try await PostgresConnection.connect(
            configuration: sourceConfig,
            id: 1,
            logger: logger
        )

        defer {
            Task {
                try? await connection.close()
            }
        }

        let introspector = PGCatalogIntrospector(connection: connection, logger: logger)
        let tableSQLGen = TableSQLGenerator()
        let permissionSQLGen = PermissionSQLGenerator()

        var steps: [CloneStep] = []

        for spec in job.objects {
            logger.info("Processing \(spec.id)")

            // Drop if exists
            if job.dropIfExists {
                steps.append(.dropObject(spec.id))
            }

            // Introspect and generate DDL
            switch spec.id.type {
            case .table:
                let metadata = try await introspector.describeTable(spec.id)
                let createSQL = try tableSQLGen.generateCreate(from: metadata)
                steps.append(.createObject(sql: createSQL, id: spec.id))

                // Data transfer
                if spec.copyData {
                    let size = try await introspector.relationSize(spec.id)
                    let method = resolveTransferMethod(
                        preferred: job.defaultDataMethod,
                        size: size,
                        threshold: job.dataSizeThreshold
                    )
                    steps.append(.copyData(id: spec.id, method: method, estimatedSize: size))
                }

                // Permissions
                if spec.copyPermissions {
                    let grants = try await introspector.permissions(for: spec.id)
                    if !grants.isEmpty {
                        let grantSQL = permissionSQLGen.generateGrants(for: spec.id, grants: grants)
                        steps.append(.grantPermissions(sql: grantSQL, id: spec.id))
                    }
                }

            default:
                logger.warning("Object type '\(spec.id.type.displayName)' not yet supported, skipping: \(spec.id)")
            }
        }

        if job.dryRun {
            let renderer = ScriptRenderer()
            return renderer.render(job: job, steps: steps)
        } else {
            // Live execution will be implemented in Phase 5
            throw PGSchemaEvoError.unsupportedObjectType(
                .table,
                reason: "Live execution not yet implemented. Use --dry-run."
            )
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
