# Getting Started with pg-schema-evo

## Prerequisites

- **Swift 6.2+** — [Install Swift](https://swift.org/install)
- **PostgreSQL client tools** — `psql`, `pg_dump`, `pg_restore` (required for data transfer)
- **Docker** (optional) — For running integration tests locally

## Installation

Build from source:

```bash
git clone https://github.com/ratranqu/pg-schema-evo.git
cd pg-schema-evo
swift build -c release
```

The binary will be at `.build/release/pg-schema-evo`.

## Your First Clone

### 1. Dry-run a single table

Start by previewing what would happen. The `--dry-run` flag outputs an executable bash script instead of making any changes:

```bash
pg-schema-evo clone \
  --source-dsn "postgresql://readonly:pass@prod-db:5432/myapp" \
  --target-dsn "postgresql://admin:pass@localhost:5432/myapp_dev" \
  --object table:public.users \
  --dry-run
```

This outputs something like:

```bash
#!/usr/bin/env bash
set -euo pipefail

TARGET_DSN="postgresql://admin:pass@localhost:5432/myapp_dev"
SOURCE_DSN="postgresql://readonly:pass@prod-db:5432/myapp"

#---------------------------------------
# 1. Create table: table:public.users
#---------------------------------------
psql "$TARGET_DSN" <<'EOSQL'
CREATE TABLE "public"."users" (
    "id" integer GENERATED ALWAYS AS IDENTITY NOT NULL,
    "username" text NOT NULL,
    "email" text NOT NULL,
    "created_at" timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT "users_pkey" PRIMARY KEY (id),
    CONSTRAINT "users_username_key" UNIQUE (username)
);
EOSQL
```

### 2. Clone with data

Add `--data` to also transfer row data:

```bash
pg-schema-evo clone \
  --source-dsn "postgresql://readonly:pass@prod-db:5432/myapp" \
  --target-dsn "postgresql://admin:pass@localhost:5432/myapp_dev" \
  --object table:public.users \
  --data \
  --dry-run
```

The script will include a `\copy` pipe command to transfer data via CSV.

### 3. Clone with permissions

Add `--permissions` to copy GRANT statements:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --data --permissions \
  --dry-run
```

### 4. Clone multiple objects

Repeat `--object` for each object:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --object table:public.orders \
  --object table:public.products \
  --data \
  --dry-run
```

### 5. Execute the script

Once you're satisfied with the dry-run output, either:

- **Pipe directly**: `pg-schema-evo clone ... --dry-run | bash`
- **Save and review**: `pg-schema-evo clone ... --dry-run > clone.sh && chmod +x clone.sh && ./clone.sh`

### 6. Re-clone with --drop-existing

To replace existing objects on the target:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --drop-existing --data \
  --dry-run
```

## Exploring the Source Database

### List objects

```bash
# All tables in public schema
pg-schema-evo list --source-dsn "..." --schema public --type table

# All object types
pg-schema-evo list --source-dsn "..."
```

### Inspect an object

```bash
pg-schema-evo inspect --source-dsn "..." --object table:public.orders
```

Shows columns, constraints, indexes, triggers, and size.

## Data Transfer Methods

pg-schema-evo supports two data transfer methods:

| Method | Flag | Best for | Script-friendly |
|--------|------|----------|----------------|
| **COPY** | `--data-method copy` | Small-medium tables (< 100 MB) | Yes |
| **pg_dump** | `--data-method pgdump` | Large tables (> 100 MB) | Partially |
| **Auto** | `--data-method auto` (default) | Let the tool decide | Yes |

The auto method checks `pg_total_relation_size` and uses COPY below the threshold (default 100 MB), pg_dump above. Override the threshold with `--data-threshold <mb>`.

## Verbosity

```bash
# Info-level logging
pg-schema-evo clone ... -v

# Debug-level logging (includes SQL queries)
pg-schema-evo clone ... -vv
```

## Next Steps

- Use YAML config files for repeatable clone jobs (coming in Phase 6)
- Use `--cascade` to auto-discover dependencies (coming in Phase 3)
- Live execution mode without `--dry-run` (coming in Phase 5)
