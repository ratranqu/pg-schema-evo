import Testing
@testable import PGSchemaEvoCore

@Suite("ParallelDataTransfer Tests")
struct ParallelDataTransferTests {

    // MARK: - Level Building

    @Test func buildLevelsNoDependencies() throws {
        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        let tasks = [
            DataTransferTask(id: .init(type: .table, schema: "public", name: "a"), method: .copy, estimatedSize: nil),
            DataTransferTask(id: .init(type: .table, schema: "public", name: "b"), method: .copy, estimatedSize: nil),
            DataTransferTask(id: .init(type: .table, schema: "public", name: "c"), method: .copy, estimatedSize: nil),
        ]

        let levels = transfer.buildLevels(tasks)
        #expect(levels.count == 1)
        #expect(levels[0].count == 3)
    }

    @Test func buildLevelsWithDependencies() throws {
        let idA = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let idB = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let idC = ObjectIdentifier(type: .table, schema: "public", name: "c")

        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        let tasks = [
            DataTransferTask(id: idA, method: .copy, estimatedSize: nil, dependsOn: []),
            DataTransferTask(id: idB, method: .copy, estimatedSize: nil, dependsOn: [idA]),
            DataTransferTask(id: idC, method: .copy, estimatedSize: nil, dependsOn: [idA, idB]),
        ]

        let levels = transfer.buildLevels(tasks)
        #expect(levels.count == 3)
        #expect(levels[0].count == 1)
        #expect(levels[0][0].id == idA)
        #expect(levels[1].count == 1)
        #expect(levels[1][0].id == idB)
        #expect(levels[2].count == 1)
        #expect(levels[2][0].id == idC)
    }

    @Test func buildLevelsPartitionsParallel() throws {
        let parent = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let child1 = ObjectIdentifier(type: .table, schema: "public", name: "orders_2024_q1")
        let child2 = ObjectIdentifier(type: .table, schema: "public", name: "orders_2024_q2")
        let child3 = ObjectIdentifier(type: .table, schema: "public", name: "orders_2024_q3")

        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        // Partition children have no dependencies (they're independent)
        let tasks = [
            DataTransferTask(id: child1, method: .copy, estimatedSize: nil, dependsOn: []),
            DataTransferTask(id: child2, method: .copy, estimatedSize: nil, dependsOn: []),
            DataTransferTask(id: child3, method: .copy, estimatedSize: nil, dependsOn: []),
        ]

        let levels = transfer.buildLevels(tasks)
        #expect(levels.count == 1)
        #expect(levels[0].count == 3)
    }

    @Test func buildLevelsMixedDependencies() throws {
        let idA = ObjectIdentifier(type: .table, schema: "public", name: "users")
        let idB = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let idC = ObjectIdentifier(type: .table, schema: "public", name: "products")
        let idD = ObjectIdentifier(type: .table, schema: "public", name: "order_items")

        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        // users and products are independent; orders depends on users; order_items depends on orders and products
        let tasks = [
            DataTransferTask(id: idA, method: .copy, estimatedSize: nil, dependsOn: []),
            DataTransferTask(id: idC, method: .copy, estimatedSize: nil, dependsOn: []),
            DataTransferTask(id: idB, method: .copy, estimatedSize: nil, dependsOn: [idA]),
            DataTransferTask(id: idD, method: .copy, estimatedSize: nil, dependsOn: [idB, idC]),
        ]

        let levels = transfer.buildLevels(tasks)
        #expect(levels.count == 3)
        // Level 0: users, products (independent)
        #expect(levels[0].count == 2)
        let level0Ids = Set(levels[0].map(\.id))
        #expect(level0Ids.contains(idA))
        #expect(level0Ids.contains(idC))
        // Level 1: orders (depends on users)
        #expect(levels[1].count == 1)
        #expect(levels[1][0].id == idB)
        // Level 2: order_items (depends on orders + products)
        #expect(levels[2].count == 1)
        #expect(levels[2][0].id == idD)
    }

    @Test func buildLevelsIgnoresExternalDependencies() throws {
        let idA = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let idExternal = ObjectIdentifier(type: .table, schema: "other", name: "external")

        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        // Task depends on an external ID that's not in the transfer set
        let tasks = [
            DataTransferTask(id: idA, method: .copy, estimatedSize: nil, dependsOn: [idExternal]),
        ]

        let levels = transfer.buildLevels(tasks)
        #expect(levels.count == 1)
        #expect(levels[0].count == 1)
    }

    @Test func buildLevelsCircularDependencies() throws {
        let idA = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let idB = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let idC = ObjectIdentifier(type: .table, schema: "public", name: "c")

        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        // A -> B -> C -> A (circular)
        let tasks = [
            DataTransferTask(id: idA, method: .copy, estimatedSize: nil, dependsOn: [idC]),
            DataTransferTask(id: idB, method: .copy, estimatedSize: nil, dependsOn: [idA]),
            DataTransferTask(id: idC, method: .copy, estimatedSize: nil, dependsOn: [idB]),
        ]

        let levels = transfer.buildLevels(tasks)
        // All tasks have circular deps, so after first empty level detection,
        // all remaining tasks are placed in one level
        let totalTasks = levels.flatMap { $0 }.count
        #expect(totalTasks == 3)
        // The circular dep fallback puts all remaining into one level
        #expect(levels.count == 1)
        #expect(levels[0].count == 3)
    }

    @Test func buildLevelsPartialCircularDependencies() throws {
        let idA = ObjectIdentifier(type: .table, schema: "public", name: "a")
        let idB = ObjectIdentifier(type: .table, schema: "public", name: "b")
        let idC = ObjectIdentifier(type: .table, schema: "public", name: "c")
        let idD = ObjectIdentifier(type: .table, schema: "public", name: "d")

        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        // A has no deps; B and C are circular; D depends on A
        let tasks = [
            DataTransferTask(id: idA, method: .copy, estimatedSize: nil, dependsOn: []),
            DataTransferTask(id: idD, method: .copy, estimatedSize: nil, dependsOn: [idA]),
            DataTransferTask(id: idB, method: .copy, estimatedSize: nil, dependsOn: [idC]),
            DataTransferTask(id: idC, method: .copy, estimatedSize: nil, dependsOn: [idB]),
        ]

        let levels = transfer.buildLevels(tasks)
        // Level 0: A (no deps)
        // Level 1: D (depends on A, now scheduled)
        // Then B and C are circular → dumped into one level
        #expect(levels.count == 3)
        #expect(levels[0].count == 1) // A
        #expect(levels[0][0].id == idA)
        #expect(levels[1].count == 1) // D
        #expect(levels[1][0].id == idD)
        #expect(levels[2].count == 2) // B and C (circular fallback)
    }

    @Test func buildLevelsEmptyTransfers() throws {
        let transfer = ParallelDataTransfer(
            maxConcurrency: 4,
            shell: ShellRunner(),
            logger: .init(label: "test")
        )

        let levels = transfer.buildLevels([])
        #expect(levels.isEmpty)
    }

    @Test func dataTransferTaskInitWithWhereAndLimit() {
        let id = ObjectIdentifier(type: .table, schema: "public", name: "orders")
        let task = DataTransferTask(
            id: id,
            method: .pgDump,
            estimatedSize: 5000,
            whereClause: "status = 'active'",
            rowLimit: 100,
            dependsOn: []
        )
        #expect(task.whereClause == "status = 'active'")
        #expect(task.rowLimit == 100)
        #expect(task.method == .pgDump)
        #expect(task.estimatedSize == 5000)
    }

    // MARK: - Auto Detect Concurrency

    @Test func autoDetectConcurrency() {
        let concurrency = ParallelDataTransfer.autoDetectConcurrency()
        #expect(concurrency >= 1)
        #expect(concurrency <= 8)
    }

    // MARK: - AsyncSemaphore

    @Test func asyncSemaphoreBasic() async {
        let sem = AsyncSemaphore(count: 2)
        #expect(sem.totalCount == 2)

        await sem.wait()
        await sem.wait()
        // Both acquired, signal one back
        sem.signal()
        await sem.wait()
        sem.signal()
        sem.signal()
    }

    // MARK: - CloneJob Parallel Field

    @Test func cloneJobDefaultParallel() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", port: 5432, database: "d", username: "u"),
            target: ConnectionConfig(host: "h", port: 5432, database: "d", username: "u"),
            objects: []
        )
        #expect(job.parallel == 0)
    }

    @Test func cloneJobCustomParallel() {
        let job = CloneJob(
            source: ConnectionConfig(host: "h", port: 5432, database: "d", username: "u"),
            target: ConnectionConfig(host: "h", port: 5432, database: "d", username: "u"),
            objects: [],
            parallel: 4
        )
        #expect(job.parallel == 4)
    }
}
