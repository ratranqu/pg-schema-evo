# Integration Test Coverage — User Features

This document maps integration tests to the **user-facing features** they exercise, organized by CLI command / workflow. Use it to identify which scenarios are covered and where gaps remain.

## Test Files

| File | Tests | Focus |
|------|-------|-------|
| `CloneIntegrationTests` | 8 | Basic clone operations |
| `AdvancedCloneIntegrationTests` | 10 | Cascade, permissions, RLS, partitions |
| `SyncIntegrationTests` | 6 | Basic sync operations |
| `AdvancedSyncIntegrationTests` | 8 | Alter, drop columns, drop extra |
| `IntrospectionIntegrationTests` | 12 | Database metadata discovery |
| `Phase4IntegrationTests` | 10 | Schema diffing, WHERE/LIMIT filters |
| `OrchestratorIntegrationTests` | 8 | Orchestrator, connection pooling |
| `SchemaMigrationIntegrationTests` | 15 | Schema migration and evolution |
| `MigrationTrackingIntegrationTests` | 12 | Migration versioning and tracking |
| `DataSyncIntegrationTests` | 8 | Data synchronization |
| `ExtendedIntegrationTests` | 10 | Config loading, diff edge cases |
| `ExtendedIntegrationTests2` | 9 | Constraint/index diffing |
| `CoverageBoostIntegrationTests` | 6 | Allow drop columns, extra paths |
| `CoverageBoost2IntegrationTests` | 8 | Object type introspection |
| `LiveExecutionIntegrationTests` | 10 | Live database execution |
| `LiveExecutionCoverageTests` | 51 | Comprehensive live execution paths |
| `FeatureCoverageIntegrationTests` | 11 | Feature gap coverage (see below) |

---

## Clone Command (`pg-schema-evo clone`)

### Object Types Cloned

| Object type | Dry-run | Live execution | With data | Test file(s) |
|---|---|---|---|---|
| Table | Yes | Yes | Yes (COPY + pg_dump) | `LiveExecutionCoverageTests`, `CloneIntegrationTests` |
| View | Yes | Yes | N/A | `LiveExecutionCoverageTests`, `LiveExecutionIntegrationTests` |
| Materialized view | Yes | Attempted | N/A | `LiveExecutionCoverageTests` |
| Sequence | Yes | Yes | N/A | `LiveExecutionIntegrationTests`, `FeatureCoverageIntegrationTests` |
| Enum | Yes | Yes | N/A | `LiveExecutionIntegrationTests` |
| Function | Yes | Yes (dry-run) | N/A | `LiveExecutionCoverageTests` |
| Procedure | Yes | Yes (dry-run) | N/A | `LiveExecutionCoverageTests` |
| Composite type | Yes | Yes | N/A | `LiveExecutionIntegrationTests` |
| Schema | Yes | Yes | N/A | `FeatureCoverageIntegrationTests` |
| Role | Yes | Yes (dry-run) | N/A | `AdvancedCloneIntegrationTests` |
| Extension | Yes | Yes | N/A | `LiveExecutionCoverageTests` |
| Foreign table | Yes | Yes (dry-run) | N/A | `LiveExecutionCoverageTests` |
| Partitioned table | Yes | Yes (dry-run) | Yes (parallel) | `LiveExecutionCoverageTests` |

### Clone Options Tested

| Option | Scenario | Test(s) |
|---|---|---|
| `--dry-run` | Preview SQL without execution | Many (most tests have a dry-run variant) |
| `--drop-existing` | Replace existing objects on target | `LiveExecutionCoverageTests` |
| `--force` | Skip confirmation prompt | All live execution tests |
| `--data` | Copy table data | `LiveExecutionCoverageTests` (COPY + pg_dump) |
| `--data-method copy` | Force text-mode COPY | `LiveExecutionCoverageTests` |
| `--data-method pgdump` | Force binary pg_dump | `LiveExecutionCoverageTests` |
| `--where` | Filter rows with SQL condition | `LiveExecutionCoverageTests`, `Phase4IntegrationTests` |
| `--row-limit` | Limit rows per table | `LiveExecutionCoverageTests`, `Phase4IntegrationTests` |
| `--global-row-limit` | Limit rows for all tables | `LiveExecutionCoverageTests` |
| `--cascade` | Auto-discover dependencies | `LiveExecutionCoverageTests`, `AdvancedCloneIntegrationTests` |
| `--permissions` | Copy GRANT statements | `LiveExecutionCoverageTests` |
| `--rls` | Copy Row Level Security policies | `LiveExecutionCoverageTests` |
| `--parallel N` | Parallel data transfer | `LiveExecutionCoverageTests` |
| `--retries N` | Retry on transient failure | `FeatureCoverageIntegrationTests` |
| `--config file.yaml` | Load job from YAML config | `FeatureCoverageIntegrationTests` |
| Multi-schema job | Objects from public + analytics | `FeatureCoverageIntegrationTests` |

---

## Sync Command (`pg-schema-evo sync`)

### Scenarios Tested

| Scenario | Mode | Test(s) |
|---|---|---|
| Create missing table on target | Live | `LiveExecutionCoverageTests` |
| Detect identical table (no-op) | Live | `LiveExecutionCoverageTests` |
| ALTER table — add missing column | Live | `LiveExecutionCoverageTests` |
| Drop extra columns (`--allow-drop-columns`) | Live | `LiveExecutionCoverageTests` |
| Skip destructive changes (warning) | Live | `LiveExecutionCoverageTests` |
| Drop extra objects (`--drop-extra`) | Live | `LiveExecutionCoverageTests` |
| Create missing extension | Live | `LiveExecutionCoverageTests` |
| SyncAll — full schema scan with ALTER | Dry-run | `LiveExecutionCoverageTests` |
| SyncAll — with `--allow-drop-columns` | Dry-run | `LiveExecutionCoverageTests` |
| SyncAll — skip destructive changes | Dry-run | `LiveExecutionCoverageTests` |
| Sync missing materialized view | Dry-run | `FeatureCoverageIntegrationTests` |
| Sync missing role | Dry-run | `FeatureCoverageIntegrationTests` |
| Object not found on either side | Live | `LiveExecutionCoverageTests` |

---

## Diff Command (`pg-schema-evo diff`)

### Scenarios Tested

| Scenario | Test(s) |
|---|---|
| Detect objects only in source | `Phase4IntegrationTests`, `FeatureCoverageIntegrationTests` |
| Detect objects only in target | `Phase4IntegrationTests` |
| Detect modified tables (column differences) | `ExtendedIntegrationTests2`, `FeatureCoverageIntegrationTests` |
| Detect modified views (definition change) | `Phase4IntegrationTests` |
| Detect modified materialized views | `FeatureCoverageIntegrationTests` |
| Detect constraint/index differences | `ExtendedIntegrationTests2` |
| Detect trigger differences | `ExtendedIntegrationTests2` |
| Detect enum label differences | `ExtendedIntegrationTests2` |
| Detect sequence parameter differences | `ExtendedIntegrationTests2` |
| Detect function definition differences | `ExtendedIntegrationTests2` |
| Generate migration SQL | `FeatureCoverageIntegrationTests` |
| Render text output | `Phase4IntegrationTests`, `FeatureCoverageIntegrationTests` |
| Diff across multiple schemas | `FeatureCoverageIntegrationTests` |

---

## Inspect & List Commands

### Object Types Introspected

| Object type | Introspection tested | Test(s) |
|---|---|---|
| Table (columns, constraints, indexes, triggers) | Yes | `IntrospectionIntegrationTests`, `ExtendedIntegrationTests2` |
| View (definition) | Yes | `IntrospectionIntegrationTests` |
| Materialized view | Yes | `CoverageBoost2IntegrationTests` |
| Sequence (parameters) | Yes | `IntrospectionIntegrationTests` |
| Enum (labels) | Yes | `IntrospectionIntegrationTests` |
| Function (signature, volatility) | Yes | `IntrospectionIntegrationTests`, `CoverageBoost2IntegrationTests` |
| Procedure | Yes | `CoverageBoost2IntegrationTests` |
| Composite type | Yes | `CoverageBoost2IntegrationTests` |
| Schema | Yes | `IntrospectionIntegrationTests` |
| Role | Yes | `IntrospectionIntegrationTests` |
| Extension | Yes | `IntrospectionIntegrationTests` |
| Foreign table (via pg_dump) | Yes | `LiveExecutionCoverageTests` |
| Aggregate (via pg_dump) | Yes | `LiveExecutionCoverageTests` |
| Partition info and children | Yes | `CoverageBoost2IntegrationTests` |
| RLS policies | Yes | `CoverageBoost2IntegrationTests` |
| Permissions (GRANT) | Yes | `CoverageBoost2IntegrationTests` |
| Dependencies (FK refs) | Yes | `IntrospectionIntegrationTests` |
| Relation size | Yes | `CoverageBoost2IntegrationTests` |
| Primary key columns | Yes | `DataSyncIntegrationTests` |
| Object listing by type/schema | Yes | `IntrospectionIntegrationTests` |

---

## Migrate Command (`pg-schema-evo migrate`)

### Scenarios Tested

| Scenario | Test(s) |
|---|---|
| Initialize migration tracking table | `MigrationTrackingIntegrationTests` |
| Apply single migration | `MigrationTrackingIntegrationTests` |
| Apply multiple migrations in order | `MigrationTrackingIntegrationTests` |
| Apply with count limit | `MigrationTrackingIntegrationTests` |
| Rollback applied migration | `MigrationTrackingIntegrationTests` |
| Rollback with empty history | `LiveExecutionCoverageTests` |
| Migration status (applied/pending/orphaned) | `MigrationTrackingIntegrationTests` |
| Apply with dry-run | `MigrationTrackingIntegrationTests` |
| Detect orphaned migrations | `MigrationTrackingIntegrationTests` |
| Checksum verification | `MigrationTrackingIntegrationTests` |
| Apply with empty directory | `CoverageBoost3UnitTests` (unit) |

---

## Data Sync Command (`pg-schema-evo data-sync`)

### Scenarios Tested

| Scenario | Test(s) |
|---|---|
| Initialize state (capture MAX tracking value) | `DataSyncIntegrationTests` |
| Reject table without primary key | `DataSyncIntegrationTests` |
| Reject invalid tracking column | `DataSyncIntegrationTests` |
| Run sync with no changes | `DataSyncIntegrationTests` |
| Upsert new and modified rows | `DataSyncIntegrationTests` |
| Dry-run (preview without execution) | `DataSyncIntegrationTests` |
| **Full end-to-end workflow** (init -> insert -> sync -> verify) | `FeatureCoverageIntegrationTests` |

---

## Configuration & Validation

### Scenarios Tested

| Scenario | Test(s) |
|---|---|
| Load YAML config file | `ExtendedIntegrationTests`, `FeatureCoverageIntegrationTests` |
| Config with data, permissions, WHERE, row_limit | `FeatureCoverageIntegrationTests` |
| Environment variable interpolation | `ExtendedIntegrationTests` |
| Config validation (missing sections) | `ExtendedIntegrationTests` |
| Preflight: source connectivity | `LiveExecutionCoverageTests` |
| Preflight: object existence on source | `LiveExecutionCoverageTests` |
| Preflight: target conflict detection | `LiveExecutionCoverageTests` |
| Preflight: various object types | `LiveExecutionCoverageTests` |

---

## Database Object Types Summary

| Object Type | Clone | Sync | Diff | Inspect | Data Sync |
|---|---|---|---|---|---|
| Table | Yes | Yes | Yes | Yes | Yes |
| Partitioned table | Yes | -- | -- | Yes | -- |
| View | Yes | Yes* | Yes | Yes | -- |
| Materialized view | Yes | Yes | Yes | Yes | -- |
| Sequence | Yes | Yes* | Yes | Yes | -- |
| Enum | Yes | Yes* | Yes | Yes | -- |
| Function | Yes | Yes* | Yes | Yes | -- |
| Procedure | Yes | -- | -- | Yes | -- |
| Composite type | Yes | Yes* | -- | Yes | -- |
| Schema | Yes | -- | -- | Yes | -- |
| Role | Yes | Yes | -- | Yes | -- |
| Extension | Yes | Yes | -- | Yes | -- |
| Foreign table | Yes | -- | -- | Yes* | -- |
| Aggregate | Yes* | -- | -- | Yes* | -- |
| Constraints | -- | -- | Yes | Yes | -- |
| Indexes | -- | -- | Yes | Yes | -- |
| Triggers | -- | -- | Yes | Yes | -- |
| RLS policies | Yes | -- | -- | Yes | -- |

`*` = tested via dry-run only or pg_dump extraction

---

## Known Gaps

These features exist in the codebase but are **not tested in integration tests**:

| Feature | Reason |
|---|---|
| Interactive confirmation prompt (`--force` omitted) | Requires stdin input; cannot automate in CI |
| `LiveExecutor.execute()` non-transaction path | Dead code — never called by orchestrators |
| Retry with actual transient failure | Requires simulating database connection failure |
| Operator type sync/diff | Exotic type; `listObjects` not implemented for operators |
| Foreign table data copy via clone orchestrator | Niche; foreign tables rarely hold local data |
