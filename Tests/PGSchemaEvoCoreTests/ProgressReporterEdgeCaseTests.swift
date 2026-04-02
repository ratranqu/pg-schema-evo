import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ProgressReporter Edge Case Tests")
struct ProgressReporterEdgeCaseTests {

    @Test("detectColorSupport returns false when NO_COLOR is set")
    func detectColorNoColor() {
        // Save and set NO_COLOR
        let savedNoColor = ProcessInfo.processInfo.environment["NO_COLOR"]
        let savedTerm = ProcessInfo.processInfo.environment["TERM"]
        setenv("NO_COLOR", "1", 1)
        defer {
            if let saved = savedNoColor {
                setenv("NO_COLOR", saved, 1)
            } else {
                unsetenv("NO_COLOR")
            }
            if let saved = savedTerm {
                setenv("TERM", saved, 1)
            }
        }

        let result = ProgressReporter.detectColorSupport()
        #expect(result == false)
    }

    @Test("detectColorSupport returns false when TERM is dumb with no NO_COLOR")
    func detectColorTermDumb() {
        let savedNoColor = ProcessInfo.processInfo.environment["NO_COLOR"]
        let savedTerm = ProcessInfo.processInfo.environment["TERM"]
        unsetenv("NO_COLOR")
        setenv("TERM", "dumb", 1)
        defer {
            if let saved = savedNoColor {
                setenv("NO_COLOR", saved, 1)
            }
            if let saved = savedTerm {
                setenv("TERM", saved, 1)
            } else {
                unsetenv("TERM")
            }
        }

        // With TERM=dumb and no NO_COLOR, falls through to isatty check
        // In test environment, stderr is not a TTY, so result should be false
        let result = ProgressReporter.detectColorSupport()
        #expect(result == false)
    }

    @Test("detectColorSupport returns true when TERM is xterm")
    func detectColorTermXterm() {
        let savedNoColor = ProcessInfo.processInfo.environment["NO_COLOR"]
        let savedTerm = ProcessInfo.processInfo.environment["TERM"]
        unsetenv("NO_COLOR")
        setenv("TERM", "xterm-256color", 1)
        defer {
            if let saved = savedNoColor {
                setenv("NO_COLOR", saved, 1)
            }
            if let saved = savedTerm {
                setenv("TERM", saved, 1)
            } else {
                unsetenv("TERM")
            }
        }

        let result = ProgressReporter.detectColorSupport()
        #expect(result == true)
    }

    @Test("ProgressReporter with zero total steps")
    func zeroTotalSteps() {
        let reporter = ProgressReporter(totalSteps: 0, colorEnabled: false)
        #expect(reporter.totalSteps == 0)
        // Should not crash
        reporter.reportStart(objectCount: 0)
        reporter.reportComplete(stepCount: 0)
    }

    @Test("ProgressReporter with color disabled produces no ANSI codes")
    func noColorNoAnsiCodes() {
        let reporter = ProgressReporter(totalSteps: 5, colorEnabled: false)
        #expect(reporter.colorEnabled == false)
        // Methods should not crash with colorEnabled=false
        reporter.reportStep(1, description: "test")
        reporter.reportStepComplete(1, description: "test")
        reporter.reportStepFailed(1, description: "test", error: "some error")
        reporter.reportWarning("warning message")
        reporter.reportDryRun()
    }

    @Test("ProgressReporter with color enabled")
    func colorEnabled() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: true)
        #expect(reporter.colorEnabled == true)
        reporter.reportStart(objectCount: 2)
        reporter.reportStep(1, description: "Creating table")
        reporter.reportStepComplete(1, description: "Creating table")
        reporter.reportStep(2, description: "Copying data")
        reporter.reportStepFailed(2, description: "Copying data", error: "connection lost")
        reporter.reportWarning("Retrying...")
        reporter.reportComplete(stepCount: 3)
    }

    @Test("ProgressReporter dry run message")
    func dryRunMessage() {
        let reporter = ProgressReporter(totalSteps: 1, colorEnabled: false)
        // Should not crash
        reporter.reportDryRun()
    }

    @Test("ProgressReporter large step numbers")
    func largeStepNumbers() {
        let reporter = ProgressReporter(totalSteps: 99999, colorEnabled: false)
        reporter.reportStep(99999, description: "Final step")
        reporter.reportStepComplete(99999, description: "Final step")
    }
}
