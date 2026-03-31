import Foundation
import Logging

/// Describes a single data transfer operation.
public struct DataTransferTask: Sendable {
    public let id: ObjectIdentifier
    public let method: TransferMethod
    public let estimatedSize: Int?
    public let whereClause: String?
    public let rowLimit: Int?
    /// IDs of tables this transfer depends on (FK references).
    public let dependsOn: Set<ObjectIdentifier>

    public init(
        id: ObjectIdentifier,
        method: TransferMethod,
        estimatedSize: Int?,
        whereClause: String? = nil,
        rowLimit: Int? = nil,
        dependsOn: Set<ObjectIdentifier> = []
    ) {
        self.id = id
        self.method = method
        self.estimatedSize = estimatedSize
        self.whereClause = whereClause
        self.rowLimit = rowLimit
        self.dependsOn = dependsOn
    }
}

/// Executes data transfers in parallel, respecting dependency ordering.
///
/// Tables with no FK dependencies between them run concurrently. Partitions of the
/// same table are always independent and run concurrently within their group.
public struct ParallelDataTransfer: Sendable {
    private let maxConcurrency: Int
    private let shell: ShellRunner
    private let logger: Logger

    public init(maxConcurrency: Int, shell: ShellRunner, logger: Logger) {
        self.maxConcurrency = maxConcurrency
        self.shell = shell
        self.logger = logger
    }

    /// Execute all data transfers with parallelism, respecting dependencies.
    public func execute(
        transfers: [DataTransferTask],
        sourceDSN: String,
        targetDSN: String,
        sourceEnv: [String: String],
        targetEnv: [String: String]
    ) async throws {
        guard !transfers.isEmpty else { return }

        if maxConcurrency <= 1 {
            // Sequential fallback
            for task in transfers {
                try await executeTransfer(
                    task, sourceDSN: sourceDSN, targetDSN: targetDSN,
                    sourceEnv: sourceEnv, targetEnv: targetEnv
                )
            }
            return
        }

        // Build dependency levels for parallel scheduling
        let levels = buildLevels(transfers)

        for level in levels {
            try await withThrowingTaskGroup(of: Void.self) { group in
                let semaphore = AsyncSemaphore(count: maxConcurrency)

                for task in level {
                    await semaphore.wait()
                    group.addTask {
                        defer { semaphore.signal() }
                        try await executeTransfer(
                            task, sourceDSN: sourceDSN, targetDSN: targetDSN,
                            sourceEnv: sourceEnv, targetEnv: targetEnv
                        )
                    }
                }

                try await group.waitForAll()
            }
        }
    }

    /// Build execution levels from dependency graph.
    /// Level 0: tasks with no dependencies. Level N: tasks whose deps are all in levels < N.
    func buildLevels(_ transfers: [DataTransferTask]) -> [[DataTransferTask]] {
        let allIds = Set(transfers.map(\.id))
        var taskMap: [ObjectIdentifier: DataTransferTask] = [:]
        for t in transfers { taskMap[t.id] = t }

        var levels: [[DataTransferTask]] = []
        var scheduled: Set<ObjectIdentifier> = []

        while scheduled.count < transfers.count {
            var level: [DataTransferTask] = []
            for task in transfers where !scheduled.contains(task.id) {
                // Only consider deps that are in our transfer set
                let relevantDeps = task.dependsOn.intersection(allIds)
                if relevantDeps.isSubset(of: scheduled) {
                    level.append(task)
                }
            }

            if level.isEmpty {
                // Remaining tasks have circular deps — schedule them all
                let remaining = transfers.filter { !scheduled.contains($0.id) }
                levels.append(remaining)
                break
            }

            for task in level { scheduled.insert(task.id) }
            levels.append(level)
        }

        return levels
    }

    /// Execute a single data transfer using streaming COPY (pipe).
    private func executeTransfer(
        _ task: DataTransferTask,
        sourceDSN: String,
        targetDSN: String,
        sourceEnv: [String: String],
        targetEnv: [String: String]
    ) async throws {
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql", exitCode: -1,
                stderr: "psql not found in PATH"
            )
        }

        logger.info("Transferring data for \(task.id) via \(task.method.rawValue)")

        switch task.method {
        case .copy, .auto:
            try await streamCopy(
                task: task, psqlPath: psqlPath,
                sourceDSN: sourceDSN, targetDSN: targetDSN,
                sourceEnv: sourceEnv, targetEnv: targetEnv
            )
        case .pgDump:
            try await streamPgDump(
                task: task,
                sourceDSN: sourceDSN, targetDSN: targetDSN,
                sourceEnv: sourceEnv, targetEnv: targetEnv
            )
        }

        logger.info("Data transfer complete for \(task.id)")
    }

    /// Stream data via psql COPY TO STDOUT | psql COPY FROM STDIN.
    private func streamCopy(
        task: DataTransferTask,
        psqlPath: String,
        sourceDSN: String,
        targetDSN: String,
        sourceEnv: [String: String],
        targetEnv: [String: String]
    ) async throws {
        let copySource: String
        if task.whereClause != nil || task.rowLimit != nil {
            var query = "SELECT * FROM \(task.id.qualifiedName)"
            if let wh = task.whereClause { query += " WHERE \(wh)" }
            if let lim = task.rowLimit { query += " LIMIT \(lim)" }
            copySource = "\\copy (\(query)) TO STDOUT WITH (FORMAT csv, HEADER)"
        } else {
            copySource = "\\copy \(task.id.qualifiedName) TO STDOUT WITH (FORMAT csv, HEADER)"
        }

        let copyTarget = "\\copy \(task.id.qualifiedName) FROM STDIN WITH (FORMAT csv, HEADER)"

        let result = try await shell.runPipe(
            sourceCommand: psqlPath,
            sourceArguments: [sourceDSN, "-c", copySource],
            sourceEnvironment: sourceEnv,
            targetCommand: psqlPath,
            targetArguments: [targetDSN, "-c", copyTarget],
            targetEnvironment: targetEnv
        )

        guard result.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "streaming COPY \(task.id)",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }
    }

    /// Stream data via pg_dump --format=plain | psql (target).
    private func streamPgDump(
        task: DataTransferTask,
        sourceDSN: String,
        targetDSN: String,
        sourceEnv: [String: String],
        targetEnv: [String: String]
    ) async throws {
        guard let pgDumpPath = shell.which("pg_dump") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "pg_dump", exitCode: -1,
                stderr: "pg_dump not found in PATH"
            )
        }
        guard let psqlPath = shell.which("psql") else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "psql", exitCode: -1,
                stderr: "psql not found in PATH"
            )
        }

        let result = try await shell.runPipe(
            sourceCommand: pgDumpPath,
            sourceArguments: [
                "--format=plain", "--data-only",
                "--table=\(task.id.qualifiedName)", sourceDSN,
            ],
            sourceEnvironment: sourceEnv,
            targetCommand: psqlPath,
            targetArguments: [targetDSN, "--set", "ON_ERROR_STOP=1", "-X"],
            targetEnvironment: targetEnv
        )

        guard result.succeeded else {
            throw PGSchemaEvoError.shellCommandFailed(
                command: "streaming pg_dump \(task.id)",
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }
    }

    /// Compute the default parallelism level.
    public static func autoDetectConcurrency() -> Int {
        min(ProcessInfo.processInfo.activeProcessorCount, 8)
    }
}
