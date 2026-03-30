import Foundation

/// Reports progress of clone operations to stderr with optional color output.
public struct ProgressReporter: Sendable {
    public let colorEnabled: Bool
    public let totalSteps: Int

    public init(totalSteps: Int, colorEnabled: Bool = ProgressReporter.detectColorSupport()) {
        self.totalSteps = totalSteps
        self.colorEnabled = colorEnabled
    }

    /// Detect whether the terminal supports color output.
    public static func detectColorSupport() -> Bool {
        // Check NO_COLOR convention (https://no-color.org)
        if ProcessInfo.processInfo.environment["NO_COLOR"] != nil {
            return false
        }
        // Check TERM
        if let term = ProcessInfo.processInfo.environment["TERM"], term != "dumb" {
            return true
        }
        // Check if stderr is a TTY
        return isatty(STDERR_FILENO) != 0
    }

    // MARK: - ANSI Colors

    private var green: String { colorEnabled ? "\u{1B}[32m" : "" }
    private var yellow: String { colorEnabled ? "\u{1B}[33m" : "" }
    private var red: String { colorEnabled ? "\u{1B}[31m" : "" }
    private var blue: String { colorEnabled ? "\u{1B}[34m" : "" }
    private var bold: String { colorEnabled ? "\u{1B}[1m" : "" }
    private var dim: String { colorEnabled ? "\u{1B}[2m" : "" }
    private var reset: String { colorEnabled ? "\u{1B}[0m" : "" }

    // MARK: - Progress Output

    public func reportStart(objectCount: Int) {
        let msg = "\(bold)\(blue)pg-schema-evo\(reset) Cloning \(objectCount) object(s)..."
        writeStderr(msg)
    }

    public func reportStep(_ step: Int, description: String) {
        let progress = "[\(step)/\(totalSteps)]"
        let msg = "\(dim)\(progress)\(reset) \(description)"
        writeStderr(msg)
    }

    public func reportStepComplete(_ step: Int, description: String) {
        let progress = "[\(step)/\(totalSteps)]"
        let msg = "\(green)✓\(reset) \(dim)\(progress)\(reset) \(description)"
        writeStderr(msg)
    }

    public func reportStepFailed(_ step: Int, description: String, error: String) {
        let progress = "[\(step)/\(totalSteps)]"
        let msg = "\(red)✗\(reset) \(dim)\(progress)\(reset) \(description): \(red)\(error)\(reset)"
        writeStderr(msg)
    }

    public func reportWarning(_ message: String) {
        let msg = "\(yellow)⚠\(reset) \(message)"
        writeStderr(msg)
    }

    public func reportComplete(stepCount: Int) {
        let msg = "\(green)\(bold)✓ Clone completed successfully.\(reset) \(stepCount) step(s) executed."
        writeStderr(msg)
    }

    public func reportDryRun() {
        let msg = "\(yellow)\(bold)Dry-run mode\(reset) — no changes will be made. Script written to stdout."
        writeStderr(msg)
    }

    public func reportConfirmation(targetDSN: String) -> String {
        let msg = "\(yellow)\(bold)WARNING:\(reset) This will execute changes on:\n  \(bold)\(targetDSN)\(reset)\n\nType 'yes' to continue, or anything else to abort: "
        // Write without trailing newline for prompt
        FileHandle.standardError.write(Data(msg.utf8))

        // Read from stdin
        guard let response = readLine(strippingNewline: true) else {
            return ""
        }
        return response
    }

    private func writeStderr(_ message: String) {
        FileHandle.standardError.write(Data((message + "\n").utf8))
    }
}
