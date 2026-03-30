import ArgumentParser
import PGSchemaEvoCore
import Logging

struct CloneCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "clone",
        abstract: "Clone database objects from source to target cluster"
    )

    @OptionGroup var source: SourceConnectionOptions
    @OptionGroup var target: TargetConnectionOptions
    @OptionGroup var objects: ObjectSpecOptions
    @OptionGroup var transfer: TransferOptions

    func run() async throws {
        // Configure logging
        let logger = Logger(label: "pg-schema-evo")

        // Parse connections
        let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)
        let targetConfig = try ConnectionConfig.fromDSN(target.targetDsn)

        // Parse objects
        guard !objects.object.isEmpty || objects.config != nil else {
            throw ValidationError("Provide at least one --object or a --config file")
        }

        var specs: [ObjectSpec] = []
        for objStr in objects.object {
            let id = try parseObjectSpecifier(objStr)
            specs.append(ObjectSpec(
                id: id,
                copyPermissions: transfer.permissions,
                copyData: transfer.data && id.type.supportsData,
                cascadeDependencies: transfer.cascade
            ))
        }

        // Parse data method
        guard let dataMethod = TransferMethod(rawValue: transfer.dataMethod) else {
            throw ValidationError("Invalid data method '\(transfer.dataMethod)'. Use: copy, pgdump, auto")
        }

        let job = CloneJob(
            source: sourceConfig,
            target: targetConfig,
            objects: specs,
            dryRun: transfer.dryRun,
            defaultDataMethod: dataMethod,
            dataSizeThreshold: transfer.dataThreshold * 1024 * 1024,
            dropIfExists: transfer.dropExisting
        )

        let orchestrator = CloneOrchestrator(logger: logger)
        let output = try await orchestrator.execute(job: job)

        if !output.isEmpty {
            print(output)
        }
    }
}
