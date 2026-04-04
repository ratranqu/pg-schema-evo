# pg-schema-evo

Selectively clone PostgreSQL database objects between clusters. Copy tables, views, functions, and more from production to development environments (or vice versa) with full control over what gets transferred.

## Features

- **Selective cloning** — Pick exactly which objects to copy: tables, views, sequences, enums, functions, composite types, and more
- **Per-object control** — Choose whether to include data, permissions, and dependencies for each object
- **Dry-run mode** — Preview the exact SQL and shell commands that would run, output as an executable bash script
- **Live execution** — Execute directly against the target database with transaction wrapping and automatic retry
- **Hybrid connectivity** — Uses PostgresNIO for fast schema introspection, shells out to `psql`/`pg_dump` for efficient data transfer
- **Configurable data transfer** — Auto-selects between `COPY` (text, script-friendly) and `pg_dump` (binary, fast) based on table size
- **YAML config files** — Define repeatable clone jobs in version-controlled config files with environment variable interpolation
- **Dependency resolution** — Automatically discovers and orders dependencies via `pg_depend`, foreign keys, and view references
- **Schema diffing** — Compare schemas between two databases and generate migration SQL
- **Pre-flight validation** — Checks connectivity, object existence, and target conflicts before execution
- **Selective data** — Filter rows with `WHERE` clauses and row limits per table or globally
- **Partitioned tables** — Automatically clones parent tables with their partitions and bound specs
- **Row-Level Security** — Optionally clone RLS policies and enable RLS on target tables
- **Retry with rollback** — Transaction-wrapped execution with configurable retry and exponential backoff
- **Schema migrations** — Generate, apply, rollback, and track versioned migrations with UP/DOWN SQL, checksum verification, and irreversible change detection
- **Migration tracking** — Record applied migrations in a configurable PostgreSQL tracking table with rollback support
- **Shell completions** — Built-in completion scripts for bash, zsh, and fish

## Quick Start

```bash
# Clone a single table (dry-run — just show the SQL)
pg-schema-evo clone \
  --source-dsn "postgresql://user:pass@prod:5432/mydb" \
  --target-dsn "postgresql://admin:pass@localhost:5432/mydb_dev" \
  --object table:public.users \
  --dry-run

# Clone with data and permissions
pg-schema-evo clone \
  --source-dsn "postgresql://user:pass@prod:5432/mydb" \
  --target-dsn "postgresql://admin:pass@localhost:5432/mydb_dev" \
  --object table:public.users \
  --data --permissions \
  --dry-run

# Clone with a WHERE filter and row limit
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.orders \
  --data --where "orders:status = 'pending'" --row-limit 1000 \
  --dry-run

# Live execution (with auto-retry and confirmation prompt)
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --data --cascade

# Use a YAML config file
pg-schema-evo clone --config clone-job.yaml

# Compare schemas between two databases
pg-schema-evo diff \
  --source-dsn "postgresql://user:pass@prod:5432/mydb" \
  --target-dsn "postgresql://admin:pass@localhost:5432/mydb_dev" \
  --schema public

# Pre-flight validation
pg-schema-evo check --config clone-job.yaml

# List all tables in a schema
pg-schema-evo list \
  --source-dsn "postgresql://user:pass@prod:5432/mydb" \
  --schema public --type table

# Inspect a table's structure
pg-schema-evo inspect \
  --source-dsn "postgresql://user:pass@prod:5432/mydb" \
  --object table:public.orders
```

## Object Specifier Format

Objects are specified as `type:schema.name`:

```
table:public.users
view:public.active_users
matview:analytics.daily_stats
function:public.calculate_total(integer)
enum:public.order_status
composite:public.address
sequence:public.invoice_number_seq
role:readonly_role
extension:pgcrypto
```

## YAML Config Files

Define repeatable clone jobs with version-controlled config files:

```yaml
source:
  host: ${SOURCE_HOST:-prod-db}
  database: myapp
  username: ${SOURCE_USER:-readonly}
  password: ${SOURCE_PASSWORD}
target:
  host: localhost
  database: myapp_dev
  username: admin
objects:
  - type: table
    schema: public
    name: users
    data: true
    permissions: true
    rls: true
  - type: table
    schema: public
    name: orders
    data: true
    where: "status = 'pending'"
    row_limit: 1000
cascade: true
dry_run: false
```

## Docker

```bash
# Build the Docker image
docker build -t pg-schema-evo .

# Run with Docker
docker run --rm pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users --dry-run
```

## Shell Completions

```bash
# Bash
pg-schema-evo --generate-completion-script bash > /etc/bash_completion.d/pg-schema-evo

# Zsh
pg-schema-evo --generate-completion-script zsh > ~/.zfunc/_pg-schema-evo

# Fish
pg-schema-evo --generate-completion-script fish > ~/.config/fish/completions/pg-schema-evo.fish
```

## Building

Requires Swift 6.2+:

```bash
swift build
```

## Testing

### Unit tests (no database required)

```bash
swift test --filter PGSchemaEvoCoreTests
```

### Integration tests (requires PostgreSQL)

Start the test databases:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Run integration tests:

```bash
swift test --filter PGSchemaEvoIntegrationTests
```

Stop the databases:

```bash
docker compose -f docker/docker-compose.yml down
```

### Code Coverage

Coverage is collected from both unit and integration tests in CI, merged into a combined report. Pull requests that reduce line coverage by more than 1% will fail the coverage check.

Generate a coverage report locally:

```bash
swift test --enable-code-coverage
BIN=$(swift build --show-bin-path)
llvm-cov report \
  -instr-profile="$(find .build -name default.profdata)" \
  "$(find "$BIN" -name 'pg-schema-evoPackageTests' -o -name '*.xctest' | head -1)" \
  -ignore-filename-regex='Tests/|\.build/'
```

| Metric | Value |
|--------|-------|
| Line coverage | **90.04%** (max 1% regression per PR) |
| Test suites | 93 suites, 1060 tests (804 unit, 256 integration) |

> Coverage is automatically updated in this README on each merge to main.

## Documentation

- [Getting Started Guide](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [Integration Test Coverage](docs/integration-test-coverage.md)
- [Detailed Architecture](ARCHITECTURE.md)

## Status

Version 0.5.1 — Robustness improvements: connection pool leak fix using `defer` in CloneOrchestrator, SyncOrchestrator, DataSyncOrchestrator, and MigrationApplicator; WHERE clause validation to prevent SQL injection; atomic state file updates in data-sync; improved MAX query failure handling; object spec validation in preflight checks; narrower error catching; comprehensive DROP SQL generation for all object types in SchemaDiffer; partition boundary replacement hardening; logging for destructive operations. 1060 tests across 93 suites at 90%+ coverage.
