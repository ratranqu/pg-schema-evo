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
}
