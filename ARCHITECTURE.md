# pg-schema-evo Architecture

## Overview

`pg-schema-evo` is a CLI tool for selectively cloning PostgreSQL database objects
between two cluster instances. It supports bidirectional copying (prod→dev or
dev→prod) with per-object options for data, permissions, and dependency resolution.

## Hybrid Introspection Approach

The tool uses a **hybrid approach** to handle the wide variety of PostgreSQL object
types, combining two introspection strategies:

### Full Catalog Introspection (PGCatalogIntrospector)

The following object types are introspected directly from `pg_catalog` system
tables via PostgresNIO. This provides the most accurate and granular metadata,
enabling precise DDL reconstruction:

| Object Type | Catalog Source | DDL Generator |
|---|---|---|
| **Tables** | `pg_class`, `pg_attribute`, `pg_constraint`, `pg_index`, `pg_trigger` | `TableSQLGenerator` |
| **Views** | `pg_class`, `pg_get_viewdef()` | `ViewSQLGenerator` |
| **Materialized Views** | `pg_class`, `pg_get_viewdef()`, `pg_index` | `ViewSQLGenerator` |
| **Sequences** | `information_schema.sequences`, `pg_depend` | `SequenceSQLGenerator` |
| **Enums** | `pg_type`, `pg_enum` | `EnumSQLGenerator` |
| **Functions** | `pg_proc`, `pg_get_functiondef()` | `FunctionSQLGenerator` |
| **Procedures** | `pg_proc`, `pg_get_functiondef()` | `FunctionSQLGenerator` |
| **Schemas** | `pg_namespace`, `pg_roles` | `SchemaSQLGenerator` |
| **Roles** | `pg_roles`, `pg_auth_members` | `SchemaSQLGenerator` |
| **Extensions** | `pg_extension`, `pg_namespace` | `SchemaSQLGenerator` |

### pg_dump-based Introspection (PgDumpIntrospector)

The following object types use `pg_dump --schema-only` for DDL extraction. These
types have complex internal representations that `pg_dump` already handles
correctly, making reimplementation in the tool unnecessary:

| Object Type | Extraction Method |
|---|---|
| **Aggregates** | `pg_dump --schema=<schema>`, filtered for `CREATE AGGREGATE` |
| **Operators** | `pg_dump --schema=<schema>`, filtered for `CREATE OPERATOR` |
| **Foreign Data Wrappers** | Full `pg_dump`, filtered for `CREATE FOREIGN DATA WRAPPER` |
| **Foreign Tables** | `pg_dump --table=<schema.name>` |

**Why this split?** Aggregates, operators, and FDW definitions involve multiple
interdependent catalog entries (e.g., `pg_aggregate` references `pg_proc` for
transition functions, `pg_operator` references multiple functions). Reconstructing
valid DDL from these catalogs is complex and error-prone. `pg_dump` already does
this correctly, so we delegate to it rather than reimplementing.

### Trade-offs

| Aspect | Full Catalog | pg_dump-based |
|---|---|---|
| **Accuracy** | Exact, field-by-field | Complete DDL from PostgreSQL itself |
| **Speed** | Fast (single queries) | Slower (spawns pg_dump process) |
| **Dependencies** | None beyond PostgresNIO | Requires `pg_dump` in PATH |
| **Granularity** | Individual fields accessible | Opaque DDL string |
| **Dry-run** | Custom SQL generation | Pass-through DDL |

## Execution Modes

### Dry-run Mode (`--dry-run`)

Outputs a complete, executable bash script to stdout. The script uses `psql`
heredocs for DDL, `\copy` pipes for CSV data transfer, and `pg_dump | pg_restore`
for large table binary transfer. Users can review, modify, and execute it manually.

### Live Mode (default)

Executes each step by **shelling out to `psql`** rather than running DDL through
PostgresNIO. This design choice ensures:

1. **Consistency**: The same SQL runs in both dry-run and live modes
2. **Robustness**: `psql` handles transaction management, encoding, and error reporting
3. **Transparency**: Every executed command is logged and could be replayed
4. **Compatibility**: No risk of PostgresNIO-specific query limitations for DDL

Data transfer uses the same mechanisms: `\copy` pipes for tables under the size
threshold, `pg_dump --format=custom | pg_restore` for larger tables.

## Dependency Resolution

When `--cascade` is enabled, the tool:

1. Queries `pg_depend` for general dependencies, `pg_constraint` for foreign keys,
   and `pg_rewrite` for view references
2. Recursively resolves transitive dependencies
3. Applies topological sort (Kahn's algorithm) to determine creation order
4. Detects cycles and reports them as errors

The natural type ordering (roles → schemas → extensions → enums → composite types →
sequences → tables → views → functions) is used as a tiebreaker within the
topological sort.

## Schema Diffing

The `diff` subcommand compares metadata between source and target databases using
`SchemaDiffer`. It produces a `SchemaDiff` report identifying:

- Objects only in source
- Objects only in target
- Objects present in both but structurally different (columns, constraints, indexes)

Output can be rendered as a text summary or as migration SQL (`--sql` flag).

## Pre-flight Validation

Before live execution, `PreflightChecker` validates:

- Source and target database connectivity
- `psql` binary availability in PATH
- Requested objects exist on the source
- No conflicting objects already exist on the target

Pre-flight checks run automatically before live execution and can be run
standalone via the `check` subcommand. Use `--skip-preflight` to bypass.

## Partitioned Table Support

Declarative partitioned tables (PostgreSQL 10+) are automatically handled:

1. The parent table is created with its `PARTITION BY` clause
2. Each child partition is created as a standalone table
3. Children are attached to the parent with their bound specs via `ALTER TABLE ... ATTACH PARTITION`

Partition metadata is introspected from `pg_partitioned_table` (strategy, partition
key) and `pg_inherits` (child partitions and bound specs).

## Row-Level Security

When `--rls` is enabled (or `rls: true` in YAML config), the tool:

1. Introspects RLS policies from `pg_policy` for each table
2. Enables RLS on the target table (`ALTER TABLE ... ENABLE ROW LEVEL SECURITY`)
3. Recreates each policy with its original definition

## Transaction Isolation

Live execution runs the entire clone within a **single psql session** for true
transaction isolation. The approach:

1. **Pre-fetch**: Source data for all COPY steps is exported first (separate processes)
2. **Build script**: A complete SQL script is assembled with `BEGIN`, all DDL/data
   inline (using `COPY ... FROM STDIN`), and `COMMIT`
3. **Execute atomically**: The script is sent to one psql process against the target

Because everything runs in one PostgreSQL session, `BEGIN`/`COMMIT` provide genuine
atomicity — either all objects are created or nothing is committed. On failure,
PostgreSQL automatically rolls back when the session disconnects.

## Retry and Rollback

- On failure: the transaction is automatically rolled back (session disconnect),
  then the tool waits with exponential backoff (2^attempt seconds) before retrying
- Default: 3 retry attempts (configurable via `--retries` or YAML config)
- On success: `COMMIT` finalizes all changes atomically

## Project Structure

```
Sources/
├── PGSchemaEvoCLI/          # CLI entry point and argument parsing
│   ├── Commands/            # clone, diff, check, inspect, list subcommands
│   └── Options/             # @OptionGroup structs (connection, transfer, objects)
└── PGSchemaEvoCore/         # Core library
    ├── Model/               # Data types (ObjectIdentifier, CloneJob, metadata)
    ├── Introspection/       # PGCatalogIntrospector, PgDumpIntrospector
    ├── SQLGen/              # DDL generators per object type
    ├── Dependencies/        # DependencyResolver with topological sort
    ├── Execution/           # CloneOrchestrator, LiveExecutor, ScriptRenderer, ShellRunner,
    │                        #   ProgressReporter, PreflightChecker
    ├── Errors/              # PGSchemaEvoError enum
    ├── Config/              # ConfigLoader (YAML parsing with env var interpolation)
    └── Diff/                # SchemaDiffer (cross-database schema comparison)

Tests/
├── PGSchemaEvoCoreTests/         # Unit tests (no database required)
└── PGSchemaEvoIntegrationTests/  # Integration tests (require PostgreSQL)
```
