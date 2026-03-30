import Testing
import PostgresNIO
import Logging
@testable import PGSchemaEvoCore

@Suite("Clone Integration Tests", .tags(.integration))
struct CloneIntegrationTests {

    @Test("Dry-run clone of users table produces valid script")
    func dryRunCloneUsers() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyPermissions: false,
                    copyData: false,
                    cascadeDependencies: false
                ),
            ],
            dryRun: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Should be a valid bash script
        #expect(script.contains("#!/usr/bin/env bash"))
        #expect(script.contains("set -euo pipefail"))
        #expect(script.contains("CREATE TABLE"))
        #expect(script.contains("\"users\""))
        // Should contain column definitions
        #expect(script.contains("username"))
        #expect(script.contains("email"))
    }

    @Test("Dry-run clone with data flag includes COPY command")
    func dryRunCloneWithData() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products"),
                    copyPermissions: false,
                    copyData: true,
                    cascadeDependencies: false
                ),
            ],
            dryRun: true,
            defaultDataMethod: .copy
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("\\copy"))
        #expect(script.contains("FORMAT csv"))
    }

    @Test("Dry-run clone with permissions includes GRANT statements")
    func dryRunCloneWithPermissions() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyPermissions: true,
                    copyData: false,
                    cascadeDependencies: false
                ),
            ],
            dryRun: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("GRANT") || script.contains("Permissions"))
    }

    @Test("Dry-run clone with drop-existing includes DROP statement")
    func dryRunCloneWithDrop() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users")
                ),
            ],
            dryRun: true,
            dropIfExists: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("DROP TABLE IF EXISTS"))
    }

    @Test("Dry-run clone of multiple tables")
    func dryRunCloneMultipleTables() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users")
                ),
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "products")
                ),
            ],
            dryRun: true
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        #expect(script.contains("\"users\""))
        #expect(script.contains("\"products\""))
    }

    @Test("Auto data method selects COPY for small tables")
    func autoMethodSelectsCopy() async throws {
        let sourceConfig = try IntegrationTestConfig.sourceConfig()
        let targetConfig = try IntegrationTestConfig.targetConfig()

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: [
                ObjectSpec(
                    id: ObjectIdentifier(type: .table, schema: "public", name: "users"),
                    copyData: true
                ),
            ],
            dryRun: true,
            defaultDataMethod: .auto,
            dataSizeThreshold: 100 * 1024 * 1024  // 100 MB - our test data is tiny
        )

        let orchestrator = CloneOrchestrator(logger: IntegrationTestConfig.logger)
        let script = try await orchestrator.execute(job: job)

        // Small table should use COPY method
        #expect(script.contains("\\copy"))
        #expect(!script.contains("pg_dump"))
    }
}
