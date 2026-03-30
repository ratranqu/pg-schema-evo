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

1. Queries `pg_depend` for each requested object to discover dependencies
2. Recursively resolves transitive dependencies
3. Applies topological sort (Kahn's algorithm) to determine creation order
4. Detects cycles and reports them as errors

The natural type ordering (roles → schemas → extensions → enums → sequences →
tables → views → functions) is used as a tiebreaker within the topological sort.

## Project Structure

```
Sources/
├── PGSchemaEvoCLI/          # CLI entry point and argument parsing
│   ├── Commands/            # clone, inspect, list subcommands
│   └── Options/             # @OptionGroup structs (connection, transfer, objects)
└── PGSchemaEvoCore/         # Core library
    ├── Model/               # Data types (ObjectIdentifier, CloneJob, metadata)
    ├── Introspection/       # PGCatalogIntrospector, PgDumpIntrospector
    ├── SQLGen/              # DDL generators per object type
    ├── Dependencies/        # DependencyResolver with topological sort
    ├── Execution/           # CloneOrchestrator, LiveExecutor, ScriptRenderer, ShellRunner
    ├── Errors/              # PGSchemaEvoError enum
    └── Config/              # YAML config file support

Tests/
├── PGSchemaEvoCoreTests/         # Unit tests (no database required)
└── PGSchemaEvoIntegrationTests/  # Integration tests (require PostgreSQL)
```
