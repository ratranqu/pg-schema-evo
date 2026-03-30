# pg-schema-evo

Selectively clone PostgreSQL database objects between clusters. Copy tables, views, functions, and more from production to development environments (or vice versa) with full control over what gets transferred.

## Features

- **Selective cloning** — Pick exactly which objects to copy: tables, views, sequences, enums, functions, and more
- **Per-object control** — Choose whether to include data, permissions, and dependencies for each object
- **Dry-run mode** — Preview the exact SQL and shell commands that would run, output as an executable bash script
- **Hybrid connectivity** — Uses PostgresNIO for fast schema introspection, shells out to `psql`/`pg_dump` for efficient data transfer
- **Configurable data transfer** — Auto-selects between `COPY` (text, script-friendly) and `pg_dump` (binary, fast) based on table size
- **YAML config files** — Define repeatable clone jobs in version-controlled config files

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
sequence:public.invoice_number_seq
role:readonly_role
extension:pgcrypto
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

## Documentation

- [Getting Started Guide](docs/getting-started.md)

## Status

Phase 1 (MVP) — Table DDL cloning with dry-run mode. See the [implementation plan](docs/architecture.md) for the full roadmap.
