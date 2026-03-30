import Logging

/// Extracts DDL for exotic PostgreSQL object types using pg_dump.
///
/// This is part of the hybrid introspection approach: common object types (tables,
/// views, functions, etc.) use direct pg_catalog queries via PGCatalogIntrospector,
/// while exotic types (aggregates, operators, FDW, foreign tables) delegate to
/// pg_dump for DDL extraction. This avoids reimplementing complex DDL reconstruction
/// logic that pg_dump already handles correctly.
///
/// See ARCHITECTURE.md for full rationale.
public struct PgDumpIntrospector: Sendable {
    private let sourceConfig: ConnectionConfig
    private let shell: ShellRunner
    private let logger: Logger

    public init(sourceConfig: ConnectionConfig, logger: Logger) {
        self.sourceConfig = sourceConfig
        self.shell = ShellRunner()
        self.logger = logger
    }

    /// Extract DDL for an object using pg_dump --schema-only.
    /// Works for aggregates, operators, foreign data wrappers, and foreign tables.
    public func extractDDL(for id: ObjectIdentifier) async throws -> PgDumpMetadata {
        guard let pgDumpPath = shell.which("pg_dump") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump",
                exitCode: -1,
                stderr: "pg_dump not found in PATH"
            )
        }

        let dsn = sourceConfig.toDSN()
        var args: [String] = ["--schema-only", "--no-owner", "--no-privileges"]

        switch id.type {
        case .foreignTable:
            guard let schema = id.schema else {
                throw PGSchemaEvoError.invalidObjectSpec("Foreign table requires a schema: \(id)")
            }
            args += ["--table", "\(schema).\(id.name)"]

        case .foreignDataWrapper:
            // pg_dump doesn't have a direct flag for FDW; use grep-based extraction
            return try await extractFDWDDL(id: id, pgDumpPath: pgDumpPath, dsn: dsn)

        case .aggregate:
            guard let schema = id.schema else {
                throw PGSchemaEvoError.invalidObjectSpec("Aggregate requires a schema: \(id)")
            }
            // pg_dump the whole schema and filter for the aggregate
            return try await extractFilteredDDL(
                id: id,
                pgDumpPath: pgDumpPath,
                dsn: dsn,
                schema: schema,
                pattern: "CREATE AGGREGATE"
            )

        case .operator:
            guard let schema = id.schema else {
                throw PGSchemaEvoError.invalidObjectSpec("Operator requires a schema: \(id)")
            }
            return try await extractFilteredDDL(
                id: id,
                pgDumpPath: pgDumpPath,
                dsn: dsn,
                schema: schema,
                pattern: "CREATE OPERATOR"
            )

        default:
            throw PGSchemaEvoError.unsupportedObjectType(
                id.type,
                reason: "PgDumpIntrospector only handles aggregates, operators, FDW, and foreign tables"
            )
        }

        args.append(dsn)

        let result = try await shell.run(
            command: pgDumpPath,
            arguments: args,
            environment: sourceConfig.environment()
        )

        guard result.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump \(args.joined(separator: " "))",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }

        let ddl = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !ddl.isEmpty else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        return PgDumpMetadata(id: id, ddl: ddl)
    }

    /// Extract FDW DDL by dumping and filtering.
    private func extractFDWDDL(id: ObjectIdentifier, pgDumpPath: String, dsn: String) async throws -> PgDumpMetadata {
        let result = try await shell.run(
            command: pgDumpPath,
            arguments: ["--schema-only", "--no-owner", "--no-privileges", dsn],
            environment: sourceConfig.environment()
        )

        guard result.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump (FDW extraction)",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }

        // Extract CREATE FOREIGN DATA WRAPPER and CREATE SERVER statements
        let lines = result.stdout.components(separatedBy: "\n")
        var ddlLines: [String] = []
        var capturing = false

        for line in lines {
            if line.contains("CREATE FOREIGN DATA WRAPPER") && line.contains(id.name) {
                capturing = true
            } else if line.contains("CREATE SERVER") && capturing {
                // Include associated server definitions
                capturing = true
            }

            if capturing {
                ddlLines.append(line)
                if line.hasSuffix(";") {
                    capturing = false
                }
            }
        }

        let ddl = ddlLines.joined(separator: "\n")
        guard !ddl.isEmpty else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        return PgDumpMetadata(id: id, ddl: ddl)
    }

    /// Extract DDL by dumping a schema and filtering for a specific CREATE pattern.
    private func extractFilteredDDL(
        id: ObjectIdentifier,
        pgDumpPath: String,
        dsn: String,
        schema: String,
        pattern: String
    ) async throws -> PgDumpMetadata {
        let result = try await shell.run(
            command: pgDumpPath,
            arguments: ["--schema-only", "--no-owner", "--no-privileges", "--schema", schema, dsn],
            environment: sourceConfig.environment()
        )

        guard result.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump (filtered extraction)",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }

        let lines = result.stdout.components(separatedBy: "\n")
        var ddlLines: [String] = []
        var capturing = false

        for line in lines {
            if line.hasPrefix(pattern) && line.contains(id.name) {
                capturing = true
            }

            if capturing {
                ddlLines.append(line)
                if line.hasSuffix(";") {
                    capturing = false
                }
            }
        }

        let ddl = ddlLines.joined(separator: "\n")
        guard !ddl.isEmpty else {
            throw PGSchemaEvoError.objectNotFound(id)
        }

        return PgDumpMetadata(id: id, ddl: ddl)
    }
}
