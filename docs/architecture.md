# Architecture

## Overview

pg-schema-evo is a Swift 6.2 CLI tool structured as two SPM targets:

- **PGSchemaEvoCLI** — Thin executable layer. Parses CLI arguments via swift-argument-parser and delegates to PGSchemaEvoCore.
- **PGSchemaEvoCore** — All business logic: schema introspection, SQL generation, dependency resolution, data transfer orchestration, schema diffing, and pre-flight validation.

## Module Structure

```
PGSchemaEvoCore/
  Model/          — Value types: ObjectIdentifier, ObjectSpec, ConnectionConfig, CloneJob
  Errors/         — PGSchemaEvoError enum
  Introspection/  — SchemaIntrospector protocol + PGCatalogIntrospector (PostgresNIO),
                    PgDumpIntrospector (shells out to pg_dump for exotic types)
  SQLGen/         — SQL generators per object type (Table, View, Sequence, Enum,
                    Function, Schema, CompositeType)
  Dependencies/   — DependencyResolver (pg_depend + FK + view deps, topological sort)
  Execution/      — CloneOrchestrator (workflow coordinator), ScriptRenderer (dry-run),
                    LiveExecutor (live mode with transactions), ShellRunner (subprocess),
                    ProgressReporter (ANSI terminal output), PreflightChecker
  Config/         — ConfigLoader (YAML parsing with env var interpolation)
  Diff/           — SchemaDiffer (compare objects between two databases)
```

## Key Design Decisions

### Hybrid connectivity
PostgresNIO handles schema introspection queries (fast, type-safe, async). Data transfer and DDL execution shell out to `psql`/`pg_dump`/`pg_restore` for efficiency and because these tools handle edge cases (encoding, large objects, binary formats) that would be complex to reimplement.

### Dry-run first
The default workflow is dry-run. The tool generates a complete, executable bash script. This is safer for production-to-dev workflows and allows review before execution.

### Live execution with transactions
Live mode executes the entire clone within a single psql session for true transaction isolation. All DDL and data transfer (via `COPY FROM STDIN`) run in one connection, so `BEGIN`/`COMMIT` provide genuine atomicity — either everything succeeds or nothing is committed. Source data is pre-fetched before the transaction script is built and executed. On failure, PostgreSQL automatically rolls back when the session disconnects. Retries (configurable, default 3 attempts) use exponential backoff and restart the full transaction cleanly.

### Protocol-based generators
Each object type has its own `SQLGenerator` implementation. Adding support for a new object type means implementing the protocol and registering it in `CloneOrchestrator`.

### Dependency resolution
Dependencies are discovered from three sources: `pg_depend` (general dependencies), `pg_constraint` (foreign keys), and `pg_rewrite` (view references). Kahn's algorithm performs topological sort with natural type ordering as tiebreaker.

### Schema diffing
`SchemaDiffer` compares metadata between source and target databases to produce a diff report. It identifies objects only in source, only in target, and structurally different objects. Can render as text summary or migration SQL.

### Pre-flight validation
Before live execution, `PreflightChecker` validates source/target connectivity, `psql` availability, source object existence, and target conflicts. Can be skipped with `--skip-preflight`.

### Partitioned table support
Declarative partitioned tables are automatically handled: the parent table is created with its `PARTITION BY` clause, each child partition is created, then attached with its bound spec.

### Row-Level Security
RLS policies can be optionally cloned (`--rls` flag or `rls: true` in YAML). The tool introspects `pg_policy` for policy definitions, enables RLS on the target table, and recreates each policy.

## Clone Flow

1. Parse CLI args or YAML config → `CloneJob`
2. Run pre-flight checks (connectivity, object existence, target conflicts)
3. Connect to source via PostgresNIO
4. If cascade: resolve dependencies via `pg_depend` + FK + view deps, topological sort
5. For each object in dependency order:
   a. Introspect metadata from `pg_catalog` (or `pg_dump` for exotic types)
   b. Generate CREATE DDL
   c. If partitioned: create parent, then children, then attach
   d. If data requested: determine transfer method (COPY vs pg_dump), apply WHERE/LIMIT filters
   e. If RLS requested: enable RLS and recreate policies
   f. If permissions requested: generate GRANT statements
6. Output: dry-run → bash script to stdout; live → execute with transaction wrapping and retry

## Roadmap

### Completed

| Phase | Description |
|-------|-------------|
| 1 | Table DDL cloning, dry-run mode, unit + integration tests, CI |
| 2 | Views, matviews, sequences, enums, functions, procedures, schemas, roles, extensions, permissions, inspect/list commands, hybrid pg_dump introspection for exotic types |
| 3 | YAML config files with env var interpolation, composite types, FK + view dependency resolution, progress output, Docker image, shell completions |
| 4 | Schema diffing (`diff` command), selective data (WHERE/row limits), pre-flight validation (`check` command), declarative partitions, RLS policies, retry with rollback, code coverage in CI |
| 5 | True transaction isolation via single-session execution, CI optimization (shared build cache, Docker build gating) |
| 6 | Graceful SIGINT/SIGTERM signal handling with automatic rollback on interrupt |
| 7 | Incremental schema sync (`sync` command) — detects changes via `SchemaDiffer`, generates ALTER/CREATE/DROP steps for delta application |

### Future Work

| Area | Description |
|------|-------------|
| Incremental data sync | Timestamp-based change detection and UPSERT for row-level deltas |
| Schema migration | Generate and apply ALTER statements from `diff` output instead of DROP/CREATE |
| Conflict resolution | Interactive merge when target has diverged from source |
| Performance | Parallel data transfer for independent tables, streaming COPY |
| Observability | Structured JSON logging, OpenTelemetry traces for clone operations |

## Subcommands

| Command | Description |
|---------|-------------|
| `clone` | Clone objects from source to target database |
| `sync` | Incrementally sync schema changes from source to target |
| `diff` | Compare schemas between two databases |
| `check` | Run pre-flight validation checks |
| `inspect` | Show detailed metadata for a single object |
| `list` | List objects in a database schema |
