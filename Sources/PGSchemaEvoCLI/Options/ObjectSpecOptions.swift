import ArgumentParser

struct ObjectSpecOptions: ParsableArguments {
    @Option(
        name: .long,
        help: ArgumentHelp(
            "Object to clone (repeatable). Format: type:schema.name",
            discussion: """
                Examples:
                  --object table:public.users
                  --object view:public.active_users
                  --object function:public.calculate_total(integer)

                Supported types: table, view, matview, sequence, enum, type,
                function, procedure, aggregate, operator, schema, role,
                extension, fdw, foreign_table
                """
        )
    )
    var object: [String] = []

    @Option(name: .long, help: "Path to YAML/JSON config file")
    var config: String?
}
