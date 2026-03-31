import Foundation

/// Result of running a shell command.
public struct ShellResult: Sendable {
    public let exitCode: Int32
    public let stdout: String
    public let stderr: String

    public var succeeded: Bool { exitCode == 0 }
}

/// Runs external processes (psql, pg_dump, pg_restore).
public struct ShellRunner: Sendable {

    public init() {}

    /// Run a command with arguments, returning the result.
    public func run(
        command: String,
        arguments: [String] = [],
        environment: [String: String] = [:],
        input: String? = nil
    ) async throws -> ShellResult {
        try await withCheckedThrowingContinuation { continuation in
            let process = Process()
            process.executableURL = URL(fileURLWithPath: command)
            process.arguments = arguments

            var env = ProcessInfo.processInfo.environment
            for (key, value) in environment {
                env[key] = value
            }
            process.environment = env

            let stdoutPipe = Pipe()
            let stderrPipe = Pipe()
            process.standardOutput = stdoutPipe
            process.standardError = stderrPipe

            if let input {
                let stdinPipe = Pipe()
                process.standardInput = stdinPipe
                stdinPipe.fileHandleForWriting.write(Data(input.utf8))
                stdinPipe.fileHandleForWriting.closeFile()
            }

            do {
                try process.run()
                SignalHandler.shared.registerProcess(process)
                process.waitUntilExit()
                SignalHandler.shared.unregisterProcess()

                let stdoutData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
                let stderrData = stderrPipe.fileHandleForReading.readDataToEndOfFile()

                let result = ShellResult(
                    exitCode: process.terminationStatus,
                    stdout: String(data: stdoutData, encoding: .utf8) ?? "",
                    stderr: String(data: stderrData, encoding: .utf8) ?? ""
                )
                continuation.resume(returning: result)
            } catch {
                continuation.resume(throwing: PGSchemaEvoError.shellCommandFailed(
                    command: ([command] + arguments).joined(separator: " "),
                    exitCode: -1,
                    stderr: error.localizedDescription
                ))
            }
        }
    }

    /// Pipe one process's stdout directly into another's stdin (streaming, no intermediate buffer).
    ///
    /// Used for streaming COPY: source psql COPY TO STDOUT → target psql COPY FROM STDIN.
    public func runPipe(
        sourceCommand: String,
        sourceArguments: [String] = [],
        sourceEnvironment: [String: String] = [:],
        targetCommand: String,
        targetArguments: [String] = [],
        targetEnvironment: [String: String] = [:]
    ) async throws -> ShellResult {
        try await withCheckedThrowingContinuation { continuation in
            let sourceProcess = Process()
            sourceProcess.executableURL = URL(fileURLWithPath: sourceCommand)
            sourceProcess.arguments = sourceArguments
            var srcEnv = ProcessInfo.processInfo.environment
            for (key, value) in sourceEnvironment { srcEnv[key] = value }
            sourceProcess.environment = srcEnv

            let targetProcess = Process()
            targetProcess.executableURL = URL(fileURLWithPath: targetCommand)
            targetProcess.arguments = targetArguments
            var tgtEnv = ProcessInfo.processInfo.environment
            for (key, value) in targetEnvironment { tgtEnv[key] = value }
            targetProcess.environment = tgtEnv

            // Connect source stdout → target stdin via pipe
            let pipe = Pipe()
            sourceProcess.standardOutput = pipe
            targetProcess.standardInput = pipe

            // Capture stderr from both processes
            let sourceStderr = Pipe()
            let targetStderr = Pipe()
            sourceProcess.standardError = sourceStderr
            targetProcess.standardError = targetStderr

            // Capture target stdout (usually empty for COPY FROM STDIN)
            let targetStdout = Pipe()
            targetProcess.standardOutput = targetStdout

            do {
                try sourceProcess.run()
                SignalHandler.shared.registerProcess(sourceProcess)
                try targetProcess.run()

                sourceProcess.waitUntilExit()
                // Close the write end so target sees EOF
                pipe.fileHandleForWriting.closeFile()
                targetProcess.waitUntilExit()
                SignalHandler.shared.unregisterProcess()

                let srcStderrData = sourceStderr.fileHandleForReading.readDataToEndOfFile()
                let tgtStderrData = targetStderr.fileHandleForReading.readDataToEndOfFile()
                let tgtStdoutData = targetStdout.fileHandleForReading.readDataToEndOfFile()

                let srcErr = String(data: srcStderrData, encoding: .utf8) ?? ""
                let tgtErr = String(data: tgtStderrData, encoding: .utf8) ?? ""

                // If source failed, report that
                if sourceProcess.terminationStatus != 0 {
                    continuation.resume(returning: ShellResult(
                        exitCode: sourceProcess.terminationStatus,
                        stdout: "",
                        stderr: "Source: \(srcErr)"
                    ))
                    return
                }

                // Otherwise report target status
                continuation.resume(returning: ShellResult(
                    exitCode: targetProcess.terminationStatus,
                    stdout: String(data: tgtStdoutData, encoding: .utf8) ?? "",
                    stderr: tgtErr
                ))
            } catch {
                continuation.resume(throwing: PGSchemaEvoError.shellCommandFailed(
                    command: "pipe(\(sourceCommand) | \(targetCommand))",
                    exitCode: -1,
                    stderr: error.localizedDescription
                ))
            }
        }
    }

    /// Find the full path to a command using `which`.
    public func which(_ command: String) -> String? {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/which")
        process.arguments = [command]
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = FileHandle.nullDevice
        do {
            try process.run()
            process.waitUntilExit()
            if process.terminationStatus == 0 {
                let data = pipe.fileHandleForReading.readDataToEndOfFile()
                return String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
            }
        } catch {}
        return nil
    }
}
