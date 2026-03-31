import ArgumentParser
import PGSchemaEvoCore

struct SourceConnectionOptions: ParsableArguments {
    @Option(name: .long, help: "Source PostgreSQL DSN (e.g. postgresql://user:pass@host:5432/dbname)")
    var sourceDsn: String = ""
}

struct TargetConnectionOptions: ParsableArguments {
    @Option(name: .long, help: "Target PostgreSQL DSN (e.g. postgresql://user:pass@host:5432/dbname)")
    var targetDsn: String = ""
}
