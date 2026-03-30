import ArgumentParser

struct TransferOptions: ParsableArguments {
    @Flag(name: .long, help: "Output SQL/shell script to stdout instead of executing")
    var dryRun: Bool = false

    @Flag(name: .long, help: "Copy table data")
    var data: Bool = false

    @Flag(name: .long, help: "Copy object permissions (GRANT statements)")
    var permissions: Bool = false

    @Flag(name: .long, help: "Auto-discover and include dependencies")
    var cascade: Bool = false

    @Option(name: .long, help: "Data transfer method: copy, pgdump, or auto (default: auto)")
    var dataMethod: String = "auto"

    @Option(name: .long, help: "Size threshold in MB for auto method selection (default: 100)")
    var dataThreshold: Int = 100

    @Flag(name: .long, help: "DROP IF EXISTS before CREATE")
    var dropExisting: Bool = false

    @Flag(name: .long, help: "Skip interactive confirmation prompt")
    var force: Bool = false

    @Flag(name: [.short, .long], help: "Enable verbose logging output")
    var verbose: Bool = false
}
