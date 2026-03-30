import ArgumentParser

@main
struct PGSchemaEvo: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "pg-schema-evo",
        abstract: "Selectively clone PostgreSQL database objects between clusters",
        discussion: """
            Clone tables, views, functions, and other database objects from one
            PostgreSQL cluster to another. Supports dry-run mode to preview the
            SQL that would be executed.
            """,
        version: "0.1.0",
        subcommands: [CloneCommand.self, InspectCommand.self, ListCommand.self]
    )
}
