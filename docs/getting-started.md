# Getting Started with pg-schema-evo

## Prerequisites

- **Swift 6.2+** — [Install Swift](https://swift.org/install)
- **PostgreSQL client tools** — `psql`, `pg_dump`, `pg_restore` (required for data transfer and live execution)
- **Docker** (optional) — For running integration tests locally or running pg-schema-evo in a container

## Installation

### Build from source

```bash
git clone https://github.com/ratranqu/pg-schema-evo.git
cd pg-schema-evo
swift build -c release
```

The binary will be at `.build/release/pg-schema-evo`.

### Docker

```bash
docker build -t pg-schema-evo .
docker run --rm pg-schema-evo clone --help
```

### Shell Completions

```bash
# Bash
pg-schema-evo --generate-completion-script bash > /etc/bash_completion.d/pg-schema-evo

# Zsh (add ~/.zfunc to your fpath in .zshrc if needed)
pg-schema-evo --generate-completion-script zsh > ~/.zfunc/_pg-schema-evo

# Fish
pg-schema-evo --generate-completion-script fish > ~/.config/fish/completions/pg-schema-evo.fish
```

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

### 5. Auto-discover dependencies with --cascade

Use `--cascade` to automatically discover and include all dependencies (foreign keys, types, sequences, view references):

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.orders \
  --data --cascade \
  --dry-run
```

This will automatically include `public.users` (FK dependency), `public.order_status` (enum dependency), and any other referenced objects.

### 6. Execute the script

Once you're satisfied with the dry-run output, either:

- **Pipe directly**: `pg-schema-evo clone ... --dry-run | bash`
- **Save and review**: `pg-schema-evo clone ... --dry-run > clone.sh && chmod +x clone.sh && ./clone.sh`

### 7. Live execution

Omit `--dry-run` to execute directly against the target database. Live mode wraps execution in a transaction with automatic retry:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --data --cascade
```

You'll see a confirmation prompt before execution. Use `--force` to skip the prompt.

### 8. Re-clone with --drop-existing

To replace existing objects on the target:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --drop-existing --data
```

## YAML Config Files

Define repeatable clone jobs in version-controlled config files:

```yaml
source:
  host: ${SOURCE_HOST:-prod-db}
  port: 5432
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
  - type: view
    schema: public
    name: active_users
  - type: enum
    schema: public
    name: order_status
cascade: true
dry_run: false
```

Environment variables in `${VAR:-default}` syntax are interpolated at load time.

Run a config-based clone:

```bash
pg-schema-evo clone --config clone-job.yaml
```

Config values can be overridden with CLI flags:

```bash
pg-schema-evo clone --config clone-job.yaml --dry-run --force
```

## Selective Data Transfer

### WHERE filters

Copy only matching rows using `--where`:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.orders \
  --data --where "orders:status = 'pending'" \
  --dry-run
```

Multiple filters for different tables:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.orders \
  --object table:public.users \
  --data \
  --where "orders:status = 'pending'" \
  --where "users:created_at > '2024-01-01'" \
  --dry-run
```

In YAML config, use the `where` key on each object:

```yaml
objects:
  - type: table
    schema: public
    name: orders
    data: true
    where: "status = 'pending'"
```

### Row limits

Limit the number of rows copied per table:

```bash
# Global row limit (applies to all tables)
pg-schema-evo clone ... --data --row-limit 1000

# Per-table row limit in YAML
# objects:
#   - type: table
#     schema: public
#     name: large_table
#     data: true
#     row_limit: 500
```

## Row-Level Security

Clone RLS policies along with tables using `--rls`:

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users \
  --data --rls
```

Or in YAML config:

```yaml
objects:
  - type: table
    schema: public
    name: users
    data: true
    rls: true
```

This will enable RLS on the target table and recreate all policies from the source.

## Partitioned Tables

Partitioned tables are handled automatically. When you clone a partitioned parent table, pg-schema-evo:

1. Creates the parent table with its `PARTITION BY` clause
2. Creates each child partition
3. Attaches children with their bound specs

```bash
pg-schema-evo clone \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.events \
  --data
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

## Schema Diffing

Compare schemas between two databases:

```bash
# Text summary
pg-schema-evo diff \
  --source-dsn "postgresql://user:pass@prod:5432/mydb" \
  --target-dsn "postgresql://admin:pass@localhost:5432/mydb_dev" \
  --schema public

# Filter by object type
pg-schema-evo diff \
  --source-dsn "..." --target-dsn "..." \
  --schema public --type table

# Generate migration SQL
pg-schema-evo diff \
  --source-dsn "..." --target-dsn "..." \
  --schema public --sql
```

## Pre-flight Validation

Check connectivity and object existence before cloning:

```bash
# With CLI arguments
pg-schema-evo check \
  --source-dsn "..." --target-dsn "..." \
  --object table:public.users

# With a config file
pg-schema-evo check --config clone-job.yaml
```

Pre-flight checks run automatically before live execution. Use `--skip-preflight` to bypass them.

## Retry and Error Handling

Live execution wraps the clone in a transaction. If a step fails:

1. The transaction is rolled back
2. The tool waits with exponential backoff (2s, 4s, 8s, ...)
3. The entire clone is retried

Configure the maximum retry count:

```bash
pg-schema-evo clone ... --retries 5
```

Set to 0 to disable retry:

```bash
pg-schema-evo clone ... --retries 0
```

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
# Verbose logging
pg-schema-evo clone ... -v
```
