# Architecture

## Overview

pg-schema-evo is a Swift 6.2 CLI tool structured as two SPM targets:

- **PGSchemaEvoCLI** — Thin executable layer. Parses CLI arguments via swift-argument-parser and delegates to PGSchemaEvoCore.
- **PGSchemaEvoCore** — All business logic: schema introspection, SQL generation, dependency resolution, data transfer orchestration, schema diffing, and pre-flight validation.

## Module Structure

```
PGSchemaEvoCLI/
  Commands/       — One file per CLI subcommand: CloneCommand, SyncCommand,
                    DiffCommand, DataSyncCommand, MigrateCommand, InspectCommand,
                    ListCommand, CheckCommand
  Options/        — Shared argument groups: ConnectionOptions, ObjectSpecOptions,
                    TransferOptions
  PGSchemaEvo.swift — @main entry point

PGSchemaEvoCore/
  Model/          — Value types: ObjectIdentifier, ObjectSpec, ConnectionConfig,
                    CloneJob, SyncJob, DataSyncJob, DatabaseObject, ObjectMetadata,
                    TransferMethod
  Errors/         — PGSchemaEvoError enum (all domain errors)
  Introspection/  — SchemaIntrospector protocol + PGCatalogIntrospector (PostgresNIO),
                    PgDumpIntrospector (shells out to pg_dump for exotic types)
  SQLGen/         — SQL generators per object type (Table, View, Sequence, Enum,
                    Function, Schema, CompositeType, Permission, Upsert) plus
                    SQLGenerator base protocol
  Dependencies/   — DependencyResolver (pg_depend + FK + view deps, topological sort)
  Execution/      — CloneOrchestrator, SyncOrchestrator, DataSyncOrchestrator
                    (workflow coordinators), ScriptRenderer (dry-run),
                    LiveExecutor (live mode with transactions),
                    ParallelDataTransfer (concurrent COPY streaming),
                    PostgresConnectionPool + PostgresConnectionHelper,
                    ShellRunner (subprocess), PreflightChecker,
                    ProgressReporter (ANSI terminal output),
                    SignalHandler (graceful SIGINT/SIGTERM with rollback)
  Config/         — ConfigLoader (YAML parsing with env var interpolation),
                    DataSyncStateStore (YAML state persistence for data-sync)
  Diff/           — SchemaDiffer (compare objects between two databases,
                    render text or migration SQL)
  Migration/      — Migration model types (Migration, MigrationSQL, MigrationConfig),
                    MigrationApplicator (apply/rollback against target DB),
                    MigrationGenerator (generate migration from SchemaDiff),
                    MigrationFileManager (YAML+SQL file pairs on disk),
                    MigrationStore (tracking table CRUD)
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

### Incremental schema sync
`SyncOrchestrator` uses `SchemaDiffer` to compare source and target, then generates ALTER/CREATE/DROP steps for delta application. Supports adding columns, dropping extra columns (opt-in via `--allow-drop-columns`), dropping extra objects (`--drop-extra`), and full `--sync-all` mode.

### Incremental data sync
`DataSyncOrchestrator` provides timestamp/ID-based change detection. It captures `MAX(tracking_column)` at init time, then on each run fetches only rows where `tracking_column > last_value`. Changes are applied to target via UPSERT using temp tables and `INSERT ... ON CONFLICT DO UPDATE`. State is persisted in a YAML file between runs.

### Schema migrations
`MigrationGenerator` creates paired YAML metadata + SQL files from `SchemaDiff` output. Each migration has UP/DOWN SQL sections plus optional CUSTOM and DATA sections. `MigrationApplicator` applies/rolls back migrations against a target database, tracking state in a `_pg_schema_evo_migrations` table with checksum verification for drift detection. `MigrationFileManager` handles the file naming convention (`YYYYMMDDHHMMSS_description/`) and sorting.

### Signal handling
`SignalHandler` installs SIGINT/SIGTERM handlers for graceful shutdown. During live execution, an interrupt triggers automatic rollback of the in-progress transaction before exiting.

### Connection pooling
`PostgresConnectionPool` manages a fixed-size pool of PostgresNIO connections for parallel introspection. `ParallelDataTransfer` uses dependency-aware scheduling to stream COPY data concurrently across multiple connections.

## Workflows

### Clone Flow

1. Parse CLI args or YAML config → `CloneJob`
2. Run pre-flight checks (connectivity, object existence, target conflicts)
3. Connect to source via PostgresNIO (pooled connections for parallel introspection)
4. If cascade: resolve dependencies via `pg_depend` + FK + view deps, topological sort
5. For each object in dependency order:
   a. Introspect metadata from `pg_catalog` (or `pg_dump` for exotic types)
   b. Generate CREATE DDL
   c. If partitioned: create parent, then children, then attach
   d. If data requested: determine transfer method (COPY vs pg_dump), apply WHERE/LIMIT filters
   e. If RLS requested: enable RLS and recreate policies
   f. If permissions requested: generate GRANT statements
6. Output: dry-run → bash script to stdout; live → execute with transaction wrapping and retry

### Sync Flow

1. Parse CLI args → `SyncJob`
2. Connect to both source and target via PostgresNIO
3. For each object (or all objects if `--sync-all`):
   a. Diff source vs target using `SchemaDiffer`
   b. Generate ALTER statements for modified objects, CREATE for missing, DROP for extra (if opted in)
4. Output: dry-run → SQL script; live → execute ALTER/CREATE/DROP steps

### Data Sync Flow

1. **Init**: Connect to source, capture `MAX(tracking_column)` per table → save to YAML state file
2. **Run**: Load state, fetch rows where `tracking_column > last_value` via `COPY ... TO STDOUT`, apply to target via temp table + UPSERT, update state file

### Migration Flow

1. **Generate**: Run `SchemaDiffer` between source and target → produce migration files (YAML metadata + SQL)
2. **Apply**: Read migration files in version order, compare checksums, execute UP SQL against target, record in tracking table
3. **Rollback**: Execute DOWN SQL for the most recently applied migration, remove from tracking table
4. **Status**: Compare applied migrations (from tracking table) with files on disk → report applied, pending, and orphaned

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
| 8 | Incremental data sync (`data-sync` command) — timestamp/ID-based change detection, UPSERT via temp tables, optional delete detection, YAML state file |
| 9 | Performance — connection pooling, parallel data transfer with dependency-aware scheduling, streaming COPY (no temp files), batched introspection queries, configurable `--parallel` concurrency with auto-detect |
| 10 | Schema migration (`migrate` command) — generate, apply, rollback, status subcommands; paired YAML metadata + SQL files with UP/DOWN/CUSTOM/DATA sections; reverse SQL generation; checksum verification; migration tracking table (`_pg_schema_evo_migrations`) |

### Future Work

| Area | Description |
|------|-------------|
| Conflict resolution | Interactive merge when target has diverged from source |
| Observability | Structured JSON logging, OpenTelemetry traces for clone operations |
| Scheduled sync | Watch for schema changes and auto-sync on a schedule or continuously |
| Multi-schema/multi-database | Batch operations across multiple schemas and databases |
| Plugin/hook system | Pre/post-clone hooks for custom transformations |
| Data masking | Mask or anonymize sensitive data during cloning |
| Web UI/dashboard | Visual interface for managing clone jobs and monitoring operations |

## Subcommands

| Command | Description |
|---------|-------------|
| `clone` | Clone objects from source to target database |
| `sync` | Incrementally sync schema changes from source to target |
| `data-sync` | Incremental row-level data sync with UPSERT and change tracking |
| `diff` | Compare schemas between two databases |
| `check` | Run pre-flight validation checks |
| `inspect` | Show detailed metadata for a single object |
| `list` | List objects in a database schema |
| `migrate` | Generate, apply, rollback, and check status of schema migrations |
