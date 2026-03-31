import PostgresNIO
import Logging

/// Validates prerequisites before executing a clone operation.
public struct PreflightChecker: Sendable {
    private let logger: Logger

    public init(logger: Logger) {
        self.logger = logger
    }

    /// Run all pre-flight checks. Returns a list of failures (empty = all passed).
    public func check(job: CloneJob) async throws -> [String] {
        var failures: [String] = []

        // 1. Check source connectivity
        logger.info("Pre-flight: checking source connectivity...")
        do {
            let conn = try await PostgresConnectionHelper.connect(config: job.source, logger: logger)
            try? await conn.close()
        } catch {
            failures.append("Cannot connect to source: \(error.localizedDescription)")
        }

        // 2. Check target connectivity
        logger.info("Pre-flight: checking target connectivity...")
        do {
            let conn = try await PostgresConnectionHelper.connect(config: job.target, logger: logger)
            try? await conn.close()
        } catch {
            failures.append("Cannot connect to target: \(error.localizedDescription)")
        }

        // If we can't connect, skip further checks
        guard failures.isEmpty else { return failures }

        // 3. Check psql availability (needed for live execution)
        if !job.dryRun {
            let shell = ShellRunner()
            if shell.which("psql") == nil {
                failures.append("psql not found in PATH (required for live execution)")
            }
        }

        // 4. Check source objects exist
        logger.info("Pre-flight: verifying objects exist in source...")
        do {
            let conn = try await PostgresConnectionHelper.connect(config: job.source, logger: logger)
            let introspector = PGCatalogIntrospector(connection: conn, logger: logger)

            let allSourceObjects = try await introspector.listObjects(schema: nil, types: nil)
            let sourceSet = Set(allSourceObjects)

            for spec in job.objects {
                if !sourceSet.contains(spec.id) {
                    // Try a more targeted check — listObjects may not cover all types
                    let found = try await verifyObjectExists(spec.id, introspector: introspector)
                    if !found {
                        failures.append("Object not found in source: \(spec.id)")
                    }
                }
            }

            try? await conn.close()
        } catch {
            failures.append("Failed to verify source objects: \(error.localizedDescription)")
        }

        // 5. Check for potential conflicts on target (if not drop-existing)
        if !job.dropIfExists {
            logger.info("Pre-flight: checking for conflicts on target...")
            do {
                let conn = try await PostgresConnectionHelper.connect(config: job.target, logger: logger)
                let introspector = PGCatalogIntrospector(connection: conn, logger: logger)

                let allTargetObjects = try await introspector.listObjects(schema: nil, types: nil)
                let targetSet = Set(allTargetObjects)

                for spec in job.objects {
                    if targetSet.contains(spec.id) {
                        failures.append("Object already exists on target: \(spec.id) (use --drop-existing to overwrite)")
                    }
                }

                try? await conn.close()
            } catch {
                failures.append("Failed to check target objects: \(error.localizedDescription)")
            }
        }

        return failures
    }

    private func verifyObjectExists(_ id: ObjectIdentifier, introspector: SchemaIntrospector) async throws -> Bool {
        do {
            switch id.type {
            case .table:
                _ = try await introspector.describeTable(id)
            case .view:
                _ = try await introspector.describeView(id)
            case .materializedView:
                _ = try await introspector.describeMaterializedView(id)
            case .sequence:
                _ = try await introspector.describeSequence(id)
            case .enum:
                _ = try await introspector.describeEnum(id)
            case .compositeType:
                _ = try await introspector.describeCompositeType(id)
            case .function, .procedure:
                _ = try await introspector.describeFunction(id)
            case .schema:
                _ = try await introspector.describeSchema(id)
            case .role:
                _ = try await introspector.describeRole(id)
            case .extension:
                _ = try await introspector.describeExtension(id)
            default:
                return true // Skip verification for exotic types
            }
            return true
        } catch is PGSchemaEvoError {
            return false
        }
    }
}
