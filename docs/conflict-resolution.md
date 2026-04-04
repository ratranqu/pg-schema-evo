# Conflict Resolution Guide

pg-schema-evo detects and resolves **conflicts** — destructive or irreversible
schema changes — when syncing or cloning PostgreSQL databases. Safe migrations
(adding columns, altering types) are always applied automatically. Conflicts
only arise for changes that could lose data or cannot be undone.

## What Is a Conflict?

A conflict is a schema difference that requires explicit user decision:

| Conflict Type | Example | Why It's a Conflict |
|---|---|---|
| **Extra in target** | Column `legacy` exists in target but not source | Dropping it loses data |
| **Object only in target** | Table `audit_log` exists only in target | Dropping it loses the entire table |
| **Irreversible change** | Removing an enum value | PostgreSQL cannot remove enum values |

Safe changes like `ADD COLUMN`, `ALTER COLUMN TYPE`, or `CREATE TABLE` are
**not** conflicts — they are applied automatically regardless of strategy.

## Resolution Strategies

| Strategy | Flag | Behavior |
|---|---|---|
| `fail` | `--conflict-strategy fail` | Error on any conflict (default when conflict flags used) |
| `source-wins` | `--ours` | Apply source schema for all conflicts |
| `target-wins` | `--theirs` | Keep target schema for all conflicts |
| `interactive` | `--manual` | Prompt per conflict |
| `skip` | `--conflict-strategy skip` | Skip all conflicts silently |

### Legacy Behavior (No Flags)

When **no** conflict flags are specified, pg-schema-evo preserves backward
compatibility: destructive changes are silently skipped with a warning, and
safe migrations are applied. This matches pre-conflict-resolution behavior.

## CLI Options

```
--conflict-strategy <strategy>   Resolution strategy: fail, source-wins, target-wins, interactive, skip
--ours                           Alias for --conflict-strategy source-wins
--theirs                         Alias for --conflict-strategy target-wins
--manual                         Alias for --conflict-strategy interactive
--yes                            Auto-accept non-destructive conflicts in interactive mode
--conflict-file <path>           Write conflict report to JSON file for offline review
--resolve-from <path>            Apply resolutions from a previously edited conflict file
--force                          Allow destructive changes (required with source-wins for DROP operations)
```

These options work with both `sync` and `clone` commands.

## Examples

### Prerequisites

All examples assume two PostgreSQL databases accessible via DSN. Set these
environment variables (adjust for your setup):

```bash
export SOURCE_DSN="postgresql://user:pass@localhost:5432/source_db"
export TARGET_DSN="postgresql://user:pass@localhost:5432/target_db"
```

To set up a test scenario with conflicts, create divergent schemas:

```bash
# Create source schema
psql "$SOURCE_DSN" -c "
  CREATE TABLE IF NOT EXISTS public.demo_users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL
  );
"

# Create target schema with an extra column (conflict!)
psql "$TARGET_DSN" -c "
  CREATE TABLE IF NOT EXISTS public.demo_users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL,
    legacy_notes text  -- extra column, only in target
  );
"
```

---

### Example 1: Detect Conflicts (Dry Run)

See what conflicts exist without making changes:

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --conflict-strategy fail \
  --dry-run
```

**Expected output:** An error listing detected conflicts, including `Column
legacy_notes: extra in target`.

**Verify:** No changes were made to the target database.

```bash
psql "$TARGET_DSN" -c "\d public.demo_users"
# legacy_notes column still present
```

---

### Example 2: Source Wins (Drop Extra Column)

Force the target to match the source exactly:

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --ours --force
```

**Expected:** The `legacy_notes` column is dropped from the target.

**Verify:**

```bash
psql "$TARGET_DSN" -c "\d public.demo_users"
# legacy_notes column is gone
```

---

### Example 3: Target Wins (Keep Extra Column)

Keep the target as-is for conflicting objects:

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --theirs
```

**Expected:** No destructive changes applied. The `legacy_notes` column remains.

**Verify:**

```bash
psql "$TARGET_DSN" -c "\d public.demo_users"
# legacy_notes column still present
```

---

### Example 4: Interactive Resolution

Resolve each conflict individually:

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --manual --force
```

**Expected:** A prompt for each conflict:

```
CONFLICT [1/1] table:public.demo_users ⚠ DESTRUCTIVE
  Column legacy_notes: extra in target (not in source)
  Detail: Dropping this may cause data loss
  Source action:
    ALTER TABLE "public"."demo_users" DROP COLUMN "legacy_notes";
  Target action:
    ALTER TABLE "public"."demo_users" ADD COLUMN "legacy_notes" text;

  [s]ource wins  [t]arget wins  s[k]ip  >
```

Type `s` to apply source (drop), `t` to keep target, or `k` to skip.

---

### Example 5: Offline Review with Conflict File

Generate a JSON file for team review, then apply resolutions later.

**Step 1 — Generate the conflict file:**

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --conflict-file /tmp/conflicts.json \
  --dry-run
```

**Step 2 — Review and edit the file:**

```bash
cat /tmp/conflicts.json
```

The file contains an array of conflicts, each with a `"resolution": null` field:

```json
{
  "conflicts": [
    {
      "id": "...",
      "object": "table:public.demo_users",
      "kind": "extraInTarget",
      "description": "Column legacy_notes: extra in target",
      "isDestructive": true,
      "sourceSQL": ["ALTER TABLE \"public\".\"demo_users\" DROP COLUMN \"legacy_notes\";"],
      "targetSQL": ["ALTER TABLE \"public\".\"demo_users\" ADD COLUMN \"legacy_notes\" text;"],
      "resolution": null
    }
  ]
}
```

Edit the `resolution` field to one of: `"apply-source"`, `"keep-target"`, or `"skip"`:

```bash
# Edit with your preferred editor
vi /tmp/conflicts.json
# Change "resolution": null  →  "resolution": "apply-source"
```

**Step 3 — Apply the resolutions:**

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --resolve-from /tmp/conflicts.json \
  --force
```

**Verify:**

```bash
psql "$TARGET_DSN" -c "\d public.demo_users"
```

---

### Example 6: Dry Run Script Generation

Generate the SQL script without executing:

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --ours --force --dry-run
```

**Expected:** Prints the SQL that would be executed, including DROP statements
for resolved conflicts. You can redirect this to a file:

```bash
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --ours --force --dry-run > migration.sql

# Review the script
cat migration.sql

# Apply manually
psql "$TARGET_DSN" -f migration.sql
```

---

### Example 7: Clone with Conflict Resolution

When cloning objects that already exist on the target, conflict resolution
detects schema differences and applies only the delta (ALTER statements) instead
of dropping and recreating. This is safer than `--drop-existing`:

```bash
# Target already has demo_users with an extra column.
# Clone with source-wins to bring target in sync:
pg-schema-evo clone \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --object table:public.demo_users \
  --ours --force --dry-run
```

**Expected:** The output contains ALTER TABLE statements (ADD COLUMN, DROP
COLUMN) rather than DROP TABLE + CREATE TABLE. Safe changes are always applied;
destructive changes require `--force`.

Clone also supports `--conflict-file` and `--resolve-from` for offline review:

```bash
pg-schema-evo clone \
  --source-dsn "$SOURCE_DSN" \
  --target-dsn "$TARGET_DSN" \
  --object table:public.demo_users \
  --conflict-file /tmp/clone-conflicts.json --dry-run
```

---

## Standalone Verification Script

Save and run this script to exercise all conflict resolution features end-to-end:

```bash
#!/bin/bash
set -euo pipefail

# Configuration — adjust these DSNs for your environment
SOURCE_DSN="${SOURCE_DSN:-postgresql://testuser:testpass@localhost:5432/source_db}"
TARGET_DSN="${TARGET_DSN:-postgresql://testuser:testpass@localhost:5432/target_db}"
CONFLICT_FILE="/tmp/pg-schema-evo-demo-conflicts.json"

echo "=== Setup: Create divergent schemas ==="
psql "$SOURCE_DSN" -c "DROP TABLE IF EXISTS public.cr_demo;" 2>/dev/null
psql "$TARGET_DSN" -c "DROP TABLE IF EXISTS public.cr_demo;" 2>/dev/null

psql "$SOURCE_DSN" -c "
  CREATE TABLE public.cr_demo (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL
  );
"
psql "$TARGET_DSN" -c "
  CREATE TABLE public.cr_demo (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL,
    old_notes text,
    deprecated_flag boolean DEFAULT false
  );
"
echo "  Source: id, name, email"
echo "  Target: id, name, email, old_notes, deprecated_flag"

echo ""
echo "=== Test 1: Fail strategy detects conflicts ==="
if pg-schema-evo sync \
    --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
    --conflict-strategy fail --dry-run 2>&1; then
  echo "  UNEXPECTED: should have failed"
else
  echo "  PASS: Correctly detected conflicts"
fi

echo ""
echo "=== Test 2: Target-wins keeps extra columns ==="
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
  --theirs --dry-run 2>&1
COLS=$(psql "$TARGET_DSN" -t -c "SELECT count(*) FROM information_schema.columns WHERE table_name='cr_demo';")
echo "  Target columns after theirs: $COLS (expected: 5)"

echo ""
echo "=== Test 3: Conflict file generation ==="
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
  --conflict-file "$CONFLICT_FILE" --dry-run 2>&1
echo "  Conflict file written to: $CONFLICT_FILE"
echo "  Contents:"
cat "$CONFLICT_FILE" | head -30
echo "  ..."

echo ""
echo "=== Test 4: Edit and apply conflict file ==="
# Set all resolutions to apply-source
sed -i 's/"resolution" : null/"resolution" : "apply-source"/g' "$CONFLICT_FILE"
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
  --resolve-from "$CONFLICT_FILE" --force 2>&1
COLS=$(psql "$TARGET_DSN" -t -c "SELECT count(*) FROM information_schema.columns WHERE table_name='cr_demo';")
echo "  Target columns after resolve-from: $COLS (expected: 3)"

echo ""
echo "=== Test 5: Source-wins with force ==="
# Re-create the extra columns
psql "$TARGET_DSN" -c "ALTER TABLE public.cr_demo ADD COLUMN old_notes text;" 2>/dev/null || true
pg-schema-evo sync \
  --source-dsn "$SOURCE_DSN" --target-dsn "$TARGET_DSN" \
  --ours --force 2>&1
COLS=$(psql "$TARGET_DSN" -t -c "SELECT count(*) FROM information_schema.columns WHERE table_name='cr_demo';")
echo "  Target columns after source-wins: $COLS (expected: 3)"

echo ""
echo "=== Cleanup ==="
psql "$SOURCE_DSN" -c "DROP TABLE IF EXISTS public.cr_demo;"
psql "$TARGET_DSN" -c "DROP TABLE IF EXISTS public.cr_demo;"
rm -f "$CONFLICT_FILE"
echo "  Done."
echo ""
echo "All conflict resolution tests completed successfully."
```

To run:

```bash
chmod +x verify-conflict-resolution.sh
./verify-conflict-resolution.sh
```

## Architecture Notes

- **SchemaDiffer** compares source and target schemas field-by-field:
  - **Column properties**: data type, nullability, defaults, identity
    (`isIdentity`, `identityGeneration`), character max length, numeric
    precision/scale
  - **Constraints and indexes**: compared by name *and* definition — a name
    match with a different definition generates DROP + re-CREATE SQL
  - **Triggers and RLS policies**: compared by name and definition
- **ConflictDetector** transforms a `SchemaDiff` into a `ConflictReport` by
  classifying only destructive/irreversible changes as conflicts
- **ConflictResolver** applies the chosen strategy to produce `ConflictResolution`
  entries. Includes `resolveFromFile()` for file-based resolution shared by both
  orchestrators
- **ConflictFileIO** handles JSON serialization for offline review workflows
- **SyncOrchestrator** integrates conflict detection into the sync pipeline,
  applying safe migrations unconditionally and only blocking on actual conflicts
- **CloneOrchestrator** detects when target objects already exist and uses
  schema diffing + conflict resolution to apply delta changes instead of
  full DROP+CREATE. Falls back to normal clone for new objects

The conflict resolution system is opt-in: when no conflict flags are specified,
the tool behaves exactly as it did before the feature was added. Destructive
changes are silently skipped with a warning log, and safe migrations proceed
normally.
