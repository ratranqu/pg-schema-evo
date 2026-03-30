# Architecture

## Overview

pg-schema-evo is a Swift 6.2 CLI tool structured as two SPM targets:

- **PGSchemaEvoCLI** — Thin executable layer. Parses CLI arguments via swift-argument-parser and delegates to PGSchemaEvoCore.
- **PGSchemaEvoCore** — All business logic: schema introspection, SQL generation, dependency resolution, data transfer orchestration.

## Module Structure

```
PGSchemaEvoCore/
  Model/          — Value types: ObjectIdentifier, ObjectSpec, ConnectionConfig, CloneJob
  Errors/         — PGSchemaEvoError enum
  Introspection/  — SchemaIntrospector protocol + PGCatalogIntrospector (PostgresNIO)
    Queries/      — Raw SQL strings for pg_catalog queries
  SQLGen/         — SQLGenerator protocol + per-type generators (Table, View, etc.)
  Dependencies/   — DependencyResolver protocol + DependencyGraph (topological sort)
  Transfer/       — DataTransferStrategy protocol + COPY/pg_dump implementations
  Execution/      — CloneOrchestrator (workflow coordinator), ScriptRenderer (dry-run),
                    LiveExecutor (live mode), ShellRunner (subprocess management)
  Config/         — YAML config file parsing
```

## Key Design Decisions

### Hybrid connectivity
PostgresNIO handles schema introspection queries (fast, type-safe, async). Data transfer shells out to `psql`/`pg_dump`/`pg_restore` for efficiency and because these tools handle edge cases (encoding, large objects, binary formats) that would be complex to reimplement.

### Dry-run first
The default workflow is dry-run. The tool generates a complete, executable bash script. This is safer for production-to-dev workflows and allows review before execution.

### Protocol-based generators
Each object type has its own `SQLGenerator` implementation. Adding support for a new object type means implementing the protocol and registering it in `CloneOrchestrator`.

## Clone Flow

1. Parse CLI args or YAML config → `CloneJob`
2. Connect to source via PostgresNIO
3. If cascade: resolve dependencies via `pg_depend`, topological sort
4. For each object in dependency order:
   a. Introspect metadata from `pg_catalog`
   b. Generate CREATE DDL
   c. If data requested: determine transfer method (COPY vs pg_dump)
   d. If permissions requested: generate GRANT statements
5. Output: dry-run → bash script to stdout; live → execute against target

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| 1 | **Current** | Table DDL cloning, dry-run mode, unit + integration tests, CI |
| 2 | Planned | Views, matviews, sequences, enums, permissions, inspect/list commands |
| 3 | Planned | Dependency resolution (pg_depend, topological sort) |
| 4 | Planned | Data transfer (COPY, pg_dump, auto-selection) |
| 5 | Planned | Live execution mode |
| 6 | Planned | YAML config files, functions, procedures, schemas, roles, extensions |
| 7 | Planned | Polish: error handling, signal handling, integration test hardening |
