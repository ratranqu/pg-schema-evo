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

    @Flag(name: .long, help: "Clone RLS policies for tables")
    var rls: Bool = false

    @Option(name: .long, help: "Maximum retry attempts for transient errors (default: 3)")
    var retries: Int = 3

    @Flag(name: .long, help: "Skip pre-flight validation checks")
    var skipPreflight: Bool = false

    @Option(name: .long, help: "Global row limit for data copy")
    var rowLimit: Int?

    @Option(
        name: .long,
        help: ArgumentHelp(
            "WHERE filter for a table's data (repeatable). Format: table_name:condition",
            discussion: """
                Examples:
                  --where "users:created_at > '2024-01-01'"
                  --where "orders:status = 'pending'"
                """
        )
    )
    var `where`: [String] = []

    @Flag(name: [.short, .long], help: "Enable verbose logging output")
    var verbose: Bool = false
}
