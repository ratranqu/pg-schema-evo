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

        // Validate object specs
        for spec in job.objects {
            if let validationError = Self.validateObjectSpec(spec.id) {
                failures.append(validationError)
            }
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
            do {
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
            } catch {
                failures.append("Failed to verify source objects: \(error.localizedDescription)")
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
                do {
                    let introspector = PGCatalogIntrospector(connection: conn, logger: logger)

                    let allTargetObjects = try await introspector.listObjects(schema: nil, types: nil)
                    let targetSet = Set(allTargetObjects)

                    for spec in job.objects {
                        if targetSet.contains(spec.id) {
                            failures.append("Object already exists on target: \(spec.id) (use --drop-existing to overwrite)")
                        }
                    }
                } catch {
                    failures.append("Failed to check target objects: \(error.localizedDescription)")
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
                // Aggregates, operators, FDWs, foreign tables: skip verification
                // as they use pg_dump for introspection
                return true
            }
            return true
        } catch let error as PGSchemaEvoError {
            if case .objectNotFound = error {
                return false
            }
            // Other PGSchemaEvoError variants (e.g. introspectionFailed) — object likely doesn't exist
            logger.debug("Unexpected error verifying \(id): \(error.localizedDescription)")
            return false
        } catch {
            // Non-PGSchemaEvoError (e.g. PostgresNIO errors) — can't determine existence
            logger.debug("Unexpected error verifying \(id): \(error.localizedDescription)")
            return false
        }
    }

    /// Validate an object spec has required fields.
    static func validateObjectSpec(_ id: ObjectIdentifier) -> String? {
        switch id.type {
        case .table, .view, .materializedView, .sequence, .enum, .compositeType,
             .function, .procedure, .foreignTable, .aggregate:
            if id.schema == nil {
                return "Object \(id) requires a schema qualifier"
            }
        case .role, .extension, .foreignDataWrapper:
            break // schema not required
        case .schema:
            break
        case .operator:
            if id.schema == nil {
                return "Object \(id) requires a schema qualifier"
            }
        }
        if id.name.isEmpty {
            return "Object name cannot be empty"
        }
        return nil
    }
}
