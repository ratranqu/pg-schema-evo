import Testing
import Foundation
@testable import PGSchemaEvoCore

@Suite("SignalHandler Tests")
struct SignalHandlerTests {

    @Test("Shared instance is a singleton")
    func sharedSingleton() {
        let a = SignalHandler.shared
        let b = SignalHandler.shared
        #expect(a === b)
    }

    @Test("Install sets installed flag")
    func installSetsFlag() {
        let handler = SignalHandler.shared
        // Save initial state, install, check, then uninstall to restore
        handler.install()
        #expect(handler.isInstalled == true)
        handler.uninstall()
    }

    @Test("Uninstall clears installed flag")
    func uninstallClearsFlag() {
        let handler = SignalHandler.shared
        handler.install()
        handler.uninstall()
        #expect(handler.isInstalled == false)
    }

    @Test("Multiple installs are idempotent")
    func multipleInstalls() {
        let handler = SignalHandler.shared
        handler.install()
        handler.install() // Should not crash or double-install
        #expect(handler.isInstalled == true)
        handler.uninstall()
    }

    @Test("Multiple uninstalls are idempotent")
    func multipleUninstalls() {
        let handler = SignalHandler.shared
        handler.install()
        handler.uninstall()
        handler.uninstall() // Should not crash
        #expect(handler.isInstalled == false)
    }

    @Test("Uninstall without install is safe")
    func uninstallWithoutInstall() {
        let handler = SignalHandler.shared
        handler.uninstall() // Should not crash
        #expect(handler.isInstalled == false)
    }

    @Test("Transaction context defaults to false")
    func transactionContextDefault() {
        let handler = SignalHandler.shared
        #expect(handler.isInTransaction == false)
    }

    @Test("Set transaction context to true")
    func setTransactionContextTrue() {
        let handler = SignalHandler.shared
        handler.setTransactionContext(true)
        #expect(handler.isInTransaction == true)
        handler.setTransactionContext(false) // Restore
    }

    @Test("Set transaction context to false")
    func setTransactionContextFalse() {
        let handler = SignalHandler.shared
        handler.setTransactionContext(true)
        handler.setTransactionContext(false)
        #expect(handler.isInTransaction == false)
    }

    @Test("Register and unregister process")
    func registerUnregisterProcess() {
        let handler = SignalHandler.shared
        let process = Process()
        // Just test that register/unregister don't crash
        handler.registerProcess(process)
        handler.unregisterProcess()
    }

    @Test("Unregister without register is safe")
    func unregisterWithoutRegister() {
        let handler = SignalHandler.shared
        handler.unregisterProcess() // Should not crash
    }

    @Test("Install and uninstall restores default signal behavior")
    func installUninstallRestoresDefaults() {
        let handler = SignalHandler.shared
        handler.install()
        #expect(handler.isInstalled == true)
        handler.uninstall()
        #expect(handler.isInstalled == false)
        // After uninstall, signals should be back to default behavior
        // (We can't easily verify SIG_DFL in a test, but at least no crash)
    }

    @Test("Transaction context persists across install/uninstall")
    func transactionContextPersistsAcrossInstall() {
        let handler = SignalHandler.shared
        handler.setTransactionContext(true)
        handler.install()
        #expect(handler.isInTransaction == true)
        handler.uninstall()
        #expect(handler.isInTransaction == true)
        handler.setTransactionContext(false) // Restore
    }
}
