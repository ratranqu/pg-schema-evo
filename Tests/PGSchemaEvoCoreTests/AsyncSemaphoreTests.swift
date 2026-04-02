import Testing
@testable import PGSchemaEvoCore

@Suite("AsyncSemaphore Tests")
struct AsyncSemaphoreTests {

    @Test("Semaphore with count 1 acts as mutex")
    func semaphoreAsMutex() async {
        let sem = AsyncSemaphore(count: 1)
        #expect(sem.totalCount == 1)

        await sem.wait()
        sem.signal()
        await sem.wait()
        sem.signal()
    }

    @Test("Semaphore with count 0 blocks until signal")
    func semaphoreBlocksWhenEmpty() async {
        let sem = AsyncSemaphore(count: 1)

        // Acquire the single permit
        await sem.wait()

        // Signal from another task to unblock
        Task {
            try? await Task.sleep(nanoseconds: 10_000_000) // 10ms
            sem.signal()
        }

        // This should block until the signal above
        await sem.wait()
        sem.signal()
    }

    @Test("Multiple concurrent waiters are serialized")
    func multipleConcurrentWaiters() async {
        let sem = AsyncSemaphore(count: 2)

        // Each task acquires then releases a permit; all 4 should complete.
        await withTaskGroup(of: Int.self) { group in
            for i in 0..<4 {
                group.addTask {
                    await sem.wait()
                    sem.signal()
                    return i
                }
            }

            var results: [Int] = []
            for await value in group {
                results.append(value)
            }
            #expect(results.count == 4)
        }
    }

    @Test("totalCount reflects initial count")
    func totalCountReflectsInit() {
        let sem3 = AsyncSemaphore(count: 3)
        #expect(sem3.totalCount == 3)

        let sem10 = AsyncSemaphore(count: 10)
        #expect(sem10.totalCount == 10)
    }

    @Test("Signal without prior wait increases count")
    func signalWithoutWait() async {
        let sem = AsyncSemaphore(count: 1)

        // Signal extra
        sem.signal()

        // Should be able to wait three times (1 initial + 1 extra signal)
        await sem.wait()
        await sem.wait()
        sem.signal()
        sem.signal()
    }
}
