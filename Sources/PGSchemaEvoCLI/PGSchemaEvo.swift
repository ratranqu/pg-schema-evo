import ArgumentParser

@main
struct PGSchemaEvo: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "pg-schema-evo",
        abstract: "Selectively clone PostgreSQL database objects between clusters",
        version: "0.1.0",
        subcommands: [CloneCommand.self, InspectCommand.self, ListCommand.self]
    )
}
