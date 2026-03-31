import Foundation
#if canImport(Glibc)
import Glibc
#endif

/// No-op signal handler used to intercept signals without ignoring them.
/// Unlike SIG_IGN, a custom handler is NOT inherited by child processes,
/// so psql and other subprocesses retain their default signal behavior.
private func noopSignalHandler(_: Int32) {
    // Intentionally empty — DispatchSource handles the actual logic
}

/// Intercepts SIGINT and SIGTERM to gracefully shut down child processes.
///
/// When a signal arrives during live execution, the handler:
/// 1. Terminates any running child process (which triggers PostgreSQL auto-rollback)
/// 2. Prints a message to stderr explaining the rollback
/// 3. Exits with 128 + signal number (standard Unix convention)
///
/// Uses a no-op C handler rather than `SIG_IGN` to prevent child processes
/// from inheriting the ignored signal disposition (POSIX inherits SIG_IGN
/// across fork/exec, but not custom handlers).
///
/// Thread-safe: all state is protected by an `NSLock`.
public final class SignalHandler: @unchecked Sendable {
    public static let shared = SignalHandler()

    private let lock = NSLock()
    private var activeProcess: Process?
    private var inTransaction = false
    private var installed = false
    private var sigintSource: DispatchSourceSignal?
    private var sigtermSource: DispatchSourceSignal?

    private init() {}

    /// Install signal handlers for SIGINT and SIGTERM.
    /// Safe to call multiple times — only installs once.
    public func install() {
        lock.lock()
        defer { lock.unlock() }

        guard !installed else { return }
        installed = true

        // Install a no-op C handler to intercept the signal (preventing default
        // termination) so the DispatchSource event handler can run. We use a
        // custom handler instead of SIG_IGN because SIG_IGN is inherited by
        // child processes, which would prevent psql from handling Ctrl+C.
        signal(SIGINT, noopSignalHandler)
        signal(SIGTERM, noopSignalHandler)

        let intSource = DispatchSource.makeSignalSource(signal: SIGINT, queue: .global())
        intSource.setEventHandler { [weak self] in
            self?.handleSignal(SIGINT)
        }
        intSource.resume()
        sigintSource = intSource

        let termSource = DispatchSource.makeSignalSource(signal: SIGTERM, queue: .global())
        termSource.setEventHandler { [weak self] in
            self?.handleSignal(SIGTERM)
        }
        termSource.resume()
        sigtermSource = termSource
    }

    /// Uninstall signal handlers and restore default behavior.
    public func uninstall() {
        lock.lock()
        defer { lock.unlock() }

        guard installed else { return }
        installed = false

        sigintSource?.cancel()
        sigtermSource?.cancel()
        sigintSource = nil
        sigtermSource = nil

        signal(SIGINT, SIG_DFL)
        signal(SIGTERM, SIG_DFL)
    }

    /// Register a running child process that should be terminated on signal.
    public func registerProcess(_ process: Process) {
        lock.lock()
        defer { lock.unlock() }
        activeProcess = process
    }

    /// Unregister the active child process (e.g., after it completes).
    public func unregisterProcess() {
        lock.lock()
        defer { lock.unlock() }
        activeProcess = nil
    }

    /// Mark whether execution is currently inside a transaction.
    /// This affects the message shown to the user on interrupt.
    public func setTransactionContext(_ value: Bool) {
        lock.lock()
        defer { lock.unlock() }
        inTransaction = value
    }

    /// Whether signal handlers are currently installed.
    public var isInstalled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return installed
    }

    /// Whether we are currently in a transaction context.
    public var isInTransaction: Bool {
        lock.lock()
        defer { lock.unlock() }
        return inTransaction
    }

    private func handleSignal(_ signum: Int32) {
        lock.lock()
        let process = activeProcess
        let txn = inTransaction
        lock.unlock()

        let signalName = signum == SIGINT ? "SIGINT" : "SIGTERM"

        // Terminate the child process — PostgreSQL auto-rolls back on disconnect
        if let process, process.isRunning {
            process.terminate()
        }

        // Print message to stderr
        let message: String
        if txn {
            message = "\n\u{1B}[31m\u{1B}[1m✗ Interrupted (\(signalName))\u{1B}[0m — transaction rolled back automatically by PostgreSQL.\n  No changes were committed to the target database.\n"
        } else {
            message = "\n\u{1B}[33m\u{1B}[1m⚠ Interrupted (\(signalName))\u{1B}[0m — operation cancelled.\n"
        }
        FileHandle.standardError.write(Data(message.utf8))

        // Exit with standard Unix convention: 128 + signal number
        _Exit(128 + signum)
    }
}
