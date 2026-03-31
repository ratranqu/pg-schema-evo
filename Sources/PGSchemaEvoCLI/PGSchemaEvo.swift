import ArgumentParser

@main
struct PGSchemaEvo: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "pg-schema-evo",
        abstract: "Selectively clone PostgreSQL database objects between clusters",
        discussion: """
            pg-schema-evo introspects PostgreSQL system catalogs to extract DDL
            for individual database objects and replays them on a target cluster.
            It supports all major PostgreSQL object types, automatic dependency
            resolution, and selective data transfer.

            Shell completions:
              pg-schema-evo --generate-completion-script bash > /etc/bash_completion.d/pg-schema-evo
              pg-schema-evo --generate-completion-script zsh > ~/.zfunc/_pg-schema-evo
              pg-schema-evo --generate-completion-script fish > ~/.config/fish/completions/pg-schema-evo.fish
            """,
        version: "0.2.0",
        subcommands: [
            CloneCommand.self,
            SyncCommand.self,
            DataSyncCommand.self,
            DiffCommand.self,
            CheckCommand.self,
            InspectCommand.self,
            ListCommand.self,
        ]
    )
}
