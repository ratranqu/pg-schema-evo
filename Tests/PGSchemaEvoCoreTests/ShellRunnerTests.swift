import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ShellRunner Tests")
struct ShellRunnerTests {
    let runner = ShellRunner()

    @Test("which finds a command that exists")
    func whichFindsExistingCommand() {
        // /bin/ls should exist on any POSIX system
        let path = runner.which("ls")
        #expect(path != nil)
        #expect(path!.contains("ls"))
    }

    @Test("which returns nil for a command that does not exist")
    func whichReturnsNilForMissing() {
        let path = runner.which("this-command-definitely-does-not-exist-xyz-42")
        #expect(path == nil)
    }

    @Test("which finds echo")
    func whichFindsEcho() {
        let path = runner.which("echo")
        #expect(path != nil)
    }

    @Test("run executes a simple command successfully")
    func runSimpleCommand() async throws {
        let result = try await runner.run(command: "/bin/echo", arguments: ["hello"])
        #expect(result.succeeded)
        #expect(result.exitCode == 0)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "hello")
        #expect(result.stderr.isEmpty)
    }

    @Test("run captures stdout from a command with multiple arguments")
    func runMultipleArguments() async throws {
        let result = try await runner.run(command: "/bin/echo", arguments: ["hello", "world"])
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "hello world")
    }

    @Test("run with input piped to stdin")
    func runWithStdin() async throws {
        // Use /usr/bin/cat to echo back stdin
        let result = try await runner.run(command: "/bin/cat", input: "piped input data")
        #expect(result.succeeded)
        #expect(result.stdout == "piped input data")
    }

    @Test("run with a failing command returns non-zero exit code")
    func runFailingCommand() async throws {
        let result = try await runner.run(command: "/bin/sh", arguments: ["-c", "exit 42"])
        #expect(!result.succeeded)
        #expect(result.exitCode == 42)
    }

    @Test("run captures stderr from a failing command")
    func runCapturesStderr() async throws {
        let result = try await runner.run(
            command: "/bin/sh",
            arguments: ["-c", "echo error_output >&2; exit 1"]
        )
        #expect(!result.succeeded)
        #expect(result.exitCode == 1)
        #expect(result.stderr.contains("error_output"))
    }

    @Test("run with custom environment variables")
    func runWithEnvironment() async throws {
        let result = try await runner.run(
            command: "/bin/sh",
            arguments: ["-c", "echo $MY_TEST_VAR"],
            environment: ["MY_TEST_VAR": "custom_value"]
        )
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "custom_value")
    }

    @Test("run throws for a non-existent executable")
    func runNonExistentExecutable() async {
        await #expect(throws: PGSchemaEvoError.self) {
            try await runner.run(command: "/nonexistent/path/to/binary")
        }
    }

    @Test("ShellResult succeeded property reflects exit code")
    func shellResultSucceeded() {
        let success = ShellResult(exitCode: 0, stdout: "", stderr: "")
        #expect(success.succeeded)

        let failure = ShellResult(exitCode: 1, stdout: "", stderr: "")
        #expect(!failure.succeeded)

        let otherFailure = ShellResult(exitCode: 127, stdout: "", stderr: "")
        #expect(!otherFailure.succeeded)
    }
}
