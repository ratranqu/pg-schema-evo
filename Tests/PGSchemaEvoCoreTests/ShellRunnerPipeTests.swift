import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ShellRunner Pipe Tests")
struct ShellRunnerPipeTests {
    let runner = ShellRunner()

    @Test("runPipe pipes stdout of source to stdin of target")
    func pipeBasic() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/echo",
            sourceArguments: ["hello world"],
            targetCommand: "/bin/cat"
        )
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "hello world")
    }

    @Test("runPipe with multiline data")
    func pipeMultiline() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/sh",
            sourceArguments: ["-c", "echo 'line1'; echo 'line2'; echo 'line3'"],
            targetCommand: "/bin/cat"
        )
        #expect(result.succeeded)
        let lines = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            .components(separatedBy: "\n")
        #expect(lines.count == 3)
        #expect(lines[0] == "line1")
        #expect(lines[1] == "line2")
        #expect(lines[2] == "line3")
    }

    @Test("runPipe source failure reports source exit code")
    func pipeSourceFailure() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/sh",
            sourceArguments: ["-c", "echo 'source_err' >&2; exit 3"],
            targetCommand: "/bin/cat"
        )
        #expect(!result.succeeded)
        #expect(result.exitCode == 3)
        #expect(result.stderr.contains("Source:"))
        #expect(result.stderr.contains("source_err"))
    }

    @Test("runPipe target failure reports target exit code")
    func pipeTargetFailure() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/echo",
            sourceArguments: ["data"],
            targetCommand: "/bin/sh",
            targetArguments: ["-c", "cat > /dev/null; echo 'target_err' >&2; exit 5"]
        )
        #expect(!result.succeeded)
        #expect(result.exitCode == 5)
        #expect(result.stderr.contains("target_err"))
    }

    @Test("runPipe with environment variables for source")
    func pipeSourceEnv() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/sh",
            sourceArguments: ["-c", "echo $SRC_VAR"],
            sourceEnvironment: ["SRC_VAR": "source_value"],
            targetCommand: "/bin/cat"
        )
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "source_value")
    }

    @Test("runPipe with environment variables for target")
    func pipeTargetEnv() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/echo",
            sourceArguments: ["input"],
            targetCommand: "/bin/sh",
            targetArguments: ["-c", "cat > /dev/null; echo $TGT_VAR"],
            targetEnvironment: ["TGT_VAR": "target_value"]
        )
        #expect(result.succeeded)
        #expect(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines) == "target_value")
    }

    @Test("runPipe with non-existent source command throws")
    func pipeNonExistentSource() async {
        await #expect(throws: PGSchemaEvoError.self) {
            try await runner.runPipe(
                sourceCommand: "/nonexistent/binary/xyz",
                targetCommand: "/bin/cat"
            )
        }
    }

    @Test("runPipe with large data transfer")
    func pipeLargeData() async throws {
        // Generate a large-ish payload (10000 lines)
        let result = try await runner.runPipe(
            sourceCommand: "/bin/sh",
            sourceArguments: ["-c", "seq 1 10000"],
            targetCommand: "/usr/bin/wc",
            targetArguments: ["-l"]
        )
        #expect(result.succeeded)
        let lineCount = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        #expect(lineCount == "10000")
    }

    @Test("runPipe target processes all source data through filter")
    func pipeWithFilter() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/sh",
            sourceArguments: ["-c", "echo 'apple'; echo 'banana'; echo 'avocado'"],
            targetCommand: "/bin/grep",
            targetArguments: ["^a"]
        )
        #expect(result.succeeded)
        let lines = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            .components(separatedBy: "\n")
        #expect(lines.count == 2)
        #expect(lines.contains("apple"))
        #expect(lines.contains("avocado"))
    }

    @Test("runPipe empty source output")
    func pipeEmptySource() async throws {
        let result = try await runner.runPipe(
            sourceCommand: "/bin/sh",
            sourceArguments: ["-c", "true"],  // produces no output
            targetCommand: "/bin/cat"
        )
        #expect(result.succeeded)
        #expect(result.stdout.isEmpty)
    }
}
