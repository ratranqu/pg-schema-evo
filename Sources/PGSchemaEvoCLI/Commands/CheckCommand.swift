import ArgumentParser
import PGSchemaEvoCore
import Logging

struct CheckCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "check",
        abstract: "Run pre-flight validation checks",
        discussion: """
            Verifies connectivity, object existence, and potential conflicts
            before running a clone operation.

            Examples:
              pg-schema-evo check --source-dsn postgresql://... --target-dsn postgresql://... --object table:public.users
              pg-schema-evo check --config clone.yaml
            """
    )

    @OptionGroup var source: SourceConnectionOptions
    @OptionGroup var target: TargetConnectionOptions
    @OptionGroup var objects: ObjectSpecOptions
    @OptionGroup var transfer: TransferOptions

    func run() async throws {
        var logger = Logger(label: "pg-schema-evo")
        logger.logLevel = transfer.verbose ? .debug : .info

        let job: CloneJob

        if let configPath = objects.config {
            let loader = ConfigLoader()
            let config = try loader.load(path: configPath)
            let sourceConfig = source.sourceDsn.isEmpty ? config.source : try ConnectionConfig.fromDSN(source.sourceDsn)
            let targetConfig = target.targetDsn.isEmpty ? config.target : try ConnectionConfig.fromDSN(target.targetDsn)

            job = CloneJob(
                source: sourceConfig,
                target: targetConfig,
                objects: config.objects,
                dryRun: true,
                dropIfExists: config.dropIfExists
            )
        } else {
            guard !source.sourceDsn.isEmpty else {
                throw ValidationError("--source-dsn is required (or use --config)")
            }
            guard !target.targetDsn.isEmpty else {
                throw ValidationError("--target-dsn is required (or use --config)")
            }

            var specs: [ObjectSpec] = []
            for objStr in objects.object {
                let id = try parseObjectSpecifier(objStr)
                specs.append(ObjectSpec(id: id))
            }

            job = CloneJob(
                source: try ConnectionConfig.fromDSN(source.sourceDsn),
                target: try ConnectionConfig.fromDSN(target.targetDsn),
                objects: specs,
                dryRun: true,
                dropIfExists: transfer.dropExisting
            )
        }

        let checker = PreflightChecker(logger: logger)
        let failures = try await checker.check(job: job)

        if failures.isEmpty {
            print("All pre-flight checks passed.")
        } else {
            print("Pre-flight checks failed:")
            for failure in failures {
                print("  - \(failure)")
            }
            throw ExitCode.failure
        }
    }
}
