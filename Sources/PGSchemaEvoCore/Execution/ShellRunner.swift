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
