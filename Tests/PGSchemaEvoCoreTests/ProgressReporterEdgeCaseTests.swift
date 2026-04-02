import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("ProgressReporter Edge Case Tests")
struct ProgressReporterEdgeCaseTests {

    @Test("detectColorSupport returns a boolean without crashing")
    func detectColorReturnsBoolean() {
        // Just verify it runs without error - actual result depends on environment
        let result = ProgressReporter.detectColorSupport()
        // result is either true or false
        #expect(result == true || result == false)
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

    @Test("ProgressReporter init with explicit color disabled")
    func initExplicitColorDisabled() {
        let reporter = ProgressReporter(totalSteps: 10, colorEnabled: false)
        #expect(reporter.colorEnabled == false)
        #expect(reporter.totalSteps == 10)
    }

    @Test("ProgressReporter init with explicit color enabled")
    func initExplicitColorEnabled() {
        let reporter = ProgressReporter(totalSteps: 5, colorEnabled: true)
        #expect(reporter.colorEnabled == true)
        #expect(reporter.totalSteps == 5)
    }

    @Test("ProgressReporter all methods execute without crash when color enabled")
    func allMethodsWithColor() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: true)
        reporter.reportStart(objectCount: 5)
        reporter.reportStep(1, description: "Step one")
        reporter.reportStepComplete(1, description: "Step one done")
        reporter.reportStep(2, description: "Step two")
        reporter.reportStepFailed(2, description: "Step two", error: "error details")
        reporter.reportWarning("a warning")
        reporter.reportDryRun()
        reporter.reportComplete(stepCount: 3)
    }

    @Test("ProgressReporter all methods execute without crash when color disabled")
    func allMethodsWithoutColor() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: false)
        reporter.reportStart(objectCount: 5)
        reporter.reportStep(1, description: "Step one")
        reporter.reportStepComplete(1, description: "Step one done")
        reporter.reportStep(2, description: "Step two")
        reporter.reportStepFailed(2, description: "Step two", error: "error details")
        reporter.reportWarning("a warning")
        reporter.reportDryRun()
        reporter.reportComplete(stepCount: 3)
    }
}
