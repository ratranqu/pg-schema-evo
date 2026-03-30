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
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = transfer.verbose ? .debug : .info

        let job: CloneJob

        if let configPath = objects.config {
            // Load from config file, with CLI flags as overrides
            let loader = ConfigLoader()
            let overrides = ConfigOverrides(
                dryRun: transfer.dryRun ? true : nil,
                data: transfer.data ? true : nil,
                permissions: transfer.permissions ? true : nil,
                cascade: transfer.cascade ? true : nil,
                dataMethod: transfer.dataMethod != "auto" ? TransferMethod(rawValue: transfer.dataMethod) : nil,
                dataThresholdMB: transfer.dataThreshold != 100 ? transfer.dataThreshold : nil,
                dropExisting: transfer.dropExisting ? true : nil,
                force: transfer.force ? true : nil
            )
            let config = try loader.load(path: configPath, overrides: overrides)

            // CLI DSN flags override config file connections
            let sourceConfig = source.sourceDsn.isEmpty
                ? config.source
                : try ConnectionConfig.fromDSN(source.sourceDsn)
            let targetConfig = target.targetDsn.isEmpty
                ? config.target
                : try ConnectionConfig.fromDSN(target.targetDsn)

            // Merge CLI --object specs with config objects
            var specs = config.objects
            for objStr in objects.object {
                let id = try parseObjectSpecifier(objStr)
                specs.append(ObjectSpec(
                    id: id,
                    copyPermissions: transfer.permissions,
                    copyData: transfer.data && id.type.supportsData,
                    cascadeDependencies: transfer.cascade
                ))
            }

            job = CloneJob(
                source: sourceConfig,
                target: targetConfig,
                objects: specs,
                dryRun: config.dryRun,
                defaultDataMethod: config.defaultDataMethod,
                dataSizeThreshold: config.dataSizeThresholdMB * 1024 * 1024,
                dropIfExists: config.dropIfExists,
                force: config.force
            )
        } else {
            // Parse from CLI args only
            guard !source.sourceDsn.isEmpty else {
                throw ValidationError("--source-dsn is required (or use --config)")
            }
            guard !target.targetDsn.isEmpty else {
                throw ValidationError("--target-dsn is required (or use --config)")
            }
            let sourceConfig = try ConnectionConfig.fromDSN(source.sourceDsn)
            let targetConfig = try ConnectionConfig.fromDSN(target.targetDsn)

            guard !objects.object.isEmpty else {
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

            guard let dataMethod = TransferMethod(rawValue: transfer.dataMethod) else {
                throw ValidationError("Invalid data method '\(transfer.dataMethod)'. Use: copy, pgdump, auto")
            }

            job = CloneJob(
                source: sourceConfig,
                target: targetConfig,
                objects: specs,
                dryRun: transfer.dryRun,
                defaultDataMethod: dataMethod,
                dataSizeThreshold: transfer.dataThreshold * 1024 * 1024,
                dropIfExists: transfer.dropExisting,
                force: transfer.force
            )
        }

        let orchestrator = CloneOrchestrator(logger: logger)
        let output = try await orchestrator.execute(job: job)

        if !output.isEmpty {
            print(output)
        }
    }
}
