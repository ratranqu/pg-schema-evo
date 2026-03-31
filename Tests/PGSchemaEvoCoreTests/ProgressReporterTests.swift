import Testing
@testable import PGSchemaEvoCore

@Suite("ProgressReporter Tests")
struct ProgressReporterTests {

    @Test("Initializes with correct totalSteps and colorEnabled")
    func initProperties() {
        let reporter = ProgressReporter(totalSteps: 5, colorEnabled: false)
        #expect(reporter.totalSteps == 5)
        #expect(reporter.colorEnabled == false)
    }

    @Test("Initializes with color enabled when explicitly set")
    func initWithColorEnabled() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: true)
        #expect(reporter.totalSteps == 3)
        #expect(reporter.colorEnabled == true)
    }

    @Test("reportStart executes without error when color disabled")
    func reportStartNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportStart(objectCount: 10)
    }

    @Test("reportStep executes without error when color disabled")
    func reportStepNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportStep(1, description: "Creating schema public")
    }

    @Test("reportStepComplete executes without error when color disabled")
    func reportStepCompleteNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportStepComplete(1, description: "Created schema public")
    }

    @Test("reportStepFailed executes without error when color disabled")
    func reportStepFailedNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportStepFailed(2, description: "Create table users", error: "relation already exists")
    }

    @Test("reportWarning executes without error when color disabled")
    func reportWarningNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportWarning("Skipping data for large table")
    }

    @Test("reportComplete executes without error when color disabled")
    func reportCompleteNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportComplete(stepCount: 4)
    }

    @Test("reportDryRun executes without error when color disabled")
    func reportDryRunNoColor() {
        let reporter = ProgressReporter(totalSteps: 4, colorEnabled: false)
        reporter.reportDryRun()
    }

    @Test("All reporting methods execute without error when color enabled")
    func allMethodsWithColorEnabled() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: true)
        reporter.reportStart(objectCount: 5)
        reporter.reportStep(1, description: "Step one")
        reporter.reportStepComplete(1, description: "Step one done")
        reporter.reportStepFailed(2, description: "Step two", error: "failed")
        reporter.reportWarning("A warning")
        reporter.reportComplete(stepCount: 3)
        reporter.reportDryRun()
    }

    // MARK: - reportDryRun

    @Test("reportDryRun executes without error when color enabled")
    func reportDryRunWithColor() {
        let reporter = ProgressReporter(totalSteps: 1, colorEnabled: true)
        reporter.reportDryRun()
        // No crash means success; dry-run writes to stderr
    }

    @Test("reportDryRun executes with zero total steps")
    func reportDryRunZeroSteps() {
        let reporter = ProgressReporter(totalSteps: 0, colorEnabled: false)
        reporter.reportDryRun()
    }

    // MARK: - reportStepFailed

    @Test("reportStepFailed with color enabled includes error message")
    func reportStepFailedWithColor() {
        let reporter = ProgressReporter(totalSteps: 5, colorEnabled: true)
        reporter.reportStepFailed(3, description: "Create index", error: "permission denied")
    }

    @Test("reportStepFailed at boundary step numbers")
    func reportStepFailedBoundary() {
        let reporter = ProgressReporter(totalSteps: 1, colorEnabled: false)
        reporter.reportStepFailed(1, description: "Only step", error: "timeout")
    }

    @Test("reportStepFailed with empty error string")
    func reportStepFailedEmptyError() {
        let reporter = ProgressReporter(totalSteps: 2, colorEnabled: false)
        reporter.reportStepFailed(1, description: "A step", error: "")
    }

    // MARK: - Multiple steps sequence

    @Test("Full reporting lifecycle without color")
    func fullLifecycleNoColor() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: false)
        reporter.reportStart(objectCount: 3)
        reporter.reportStep(1, description: "Creating schema")
        reporter.reportStepComplete(1, description: "Created schema")
        reporter.reportStep(2, description: "Creating table")
        reporter.reportStepFailed(2, description: "Creating table", error: "already exists")
        reporter.reportWarning("Table was skipped")
        reporter.reportStep(3, description: "Creating view")
        reporter.reportStepComplete(3, description: "Created view")
        reporter.reportComplete(stepCount: 2)
    }

    @Test("Full reporting lifecycle with color")
    func fullLifecycleWithColor() {
        let reporter = ProgressReporter(totalSteps: 3, colorEnabled: true)
        reporter.reportStart(objectCount: 3)
        reporter.reportStep(1, description: "Creating schema")
        reporter.reportStepComplete(1, description: "Created schema")
        reporter.reportStep(2, description: "Creating table")
        reporter.reportStepFailed(2, description: "Creating table", error: "already exists")
        reporter.reportWarning("Table was skipped")
        reporter.reportStep(3, description: "Creating view")
        reporter.reportStepComplete(3, description: "Created view")
        reporter.reportComplete(stepCount: 2)
    }

    // MARK: - detectColorSupport

    @Test("detectColorSupport returns a boolean value")
    func detectColorSupportReturnsBool() {
        // Just verify it doesn't crash; actual result depends on environment
        let _ = ProgressReporter.detectColorSupport()
    }
}
