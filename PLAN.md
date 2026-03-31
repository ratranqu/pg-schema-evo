# Performance Improvements Plan

## Overview

Comprehensive performance improvements for pg-schema-evo covering:
1. Connection pooling
2. Batched introspection queries
3. Parallel data transfer (independent tables + partitions)
4. Streaming COPY (no temp files / memory buffers)
5. Configurable concurrency with auto-detect

## Architecture

### 1. Connection Pool (`PostgresConnectionPool`)

**File:** `Sources/PGSchemaEvoCore/Execution/PostgresConnectionPool.swift` (new)

Replace the single `PostgresConnection` with a pool that supports concurrent operations.

```
PostgresConnectionPool
  - init(config: ConnectionConfig, size: Int, logger: Logger)
  - withConnection<T>((PostgresConnection) async throws -> T) async throws -> T
  - close() async
```

Pool size = `--parallel` value (or auto-detect). Each parallel introspection or data transfer task borrows a connection.

**Impact:** `PGCatalogIntrospector` changes from holding a single connection to accepting a pool. Each method call borrows a connection for its duration.

### 2. Batched Introspection

**File:** Modify `PGCatalogIntrospector.swift`

Currently `describeTable` makes 4 sequential queries (columns, constraints, indexes, triggers). With a connection pool, run all 4 concurrently via `withTaskGroup`:

```swift
func describeTable(_ id: ObjectIdentifier) async throws -> TableMetadata {
    try await withThrowingTaskGroup(of: TableComponent.self) { group in
        group.addTask { .columns(try await pool.withConnection { ... }) }
        group.addTask { .constraints(try await pool.withConnection { ... }) }
        group.addTask { .indexes(try await pool.withConnection { ... }) }
        group.addTask { .triggers(try await pool.withConnection { ... }) }
        // collect results
    }
}
```

This cuts describeTable from 4 round-trips to 1 wall-clock round-trip (4 concurrent queries).

### 3. Streaming COPY (ShellRunner.runPipe)

**File:** Modify `ShellRunner.swift`

Add a `runPipe` method that connects two processes: source psql COPY TO STDOUT → target psql COPY FROM STDIN. Uses Unix pipe (source stdout → target stdin) with no intermediate memory buffer.

```swift
func runPipe(
    sourceCommand: String, sourceArgs: [String], sourceEnv: [String: String],
    targetCommand: String, targetArgs: [String], targetEnv: [String: String]
) async throws -> ShellResult
```

Implementation: create a `Pipe()`, set as source's stdout and target's stdin. Start both processes, wait for both. Data streams directly between them.

**Impact:** Replaces both `fetchSourceData` (memory buffer) and `copyViaPsqlPipe` (two-step export/import) with a single streaming pipe.

### 4. Parallel Data Transfer

**File:** `Sources/PGSchemaEvoCore/Execution/ParallelDataTransfer.swift` (new)

Uses the dependency graph to determine which tables can be transferred concurrently:

1. **Build transfer groups:** From the topologically sorted dependency graph, identify independent sets of tables (no FK relationships between them).
2. **Partition parallelism:** Children of the same partitioned table are always independent — transfer them as a group.
3. **Execute with TaskGroup:** Run up to `maxConcurrency` streaming COPY operations simultaneously.

```
ParallelDataTransfer
  - init(maxConcurrency: Int, shell: ShellRunner, logger: Logger)
  - execute(transfers: [DataTransferTask], dependencies: DependencyGraph) async throws
```

`DataTransferTask` encapsulates: source DSN, target DSN, object ID, method, where/limit.

**Scheduling:** Level-based execution from the dependency graph:
- Level 0: tables with no dependencies (all parallel)
- Level 1: tables depending only on level-0 (all parallel after level 0 completes)
- Within each level, partition children are parallelized

### 5. Concurrency Configuration

**Files:** Modify `CloneJob.swift`, `CloneCommand.swift`, `ConfigLoader.swift`

Add `--parallel N` flag:
- `0` (default) = auto-detect: `min(ProcessInfo.processInfo.activeProcessorCount, 8)`
- `1` = sequential (current behavior)
- `N` = up to N concurrent data transfers

Also supported in YAML config: `parallel: 4`

### 6. CloneOrchestrator Integration

**File:** Modify `CloneOrchestrator.swift`

Split execution into two phases:
1. **DDL phase** (sequential, in transaction): DROP/CREATE/ALTER/ATTACH, permissions, RLS
2. **Data phase** (parallel, outside transaction): streaming COPY for all tables

DDL must remain sequential in a transaction for correctness. Data transfer happens after DDL completes, using parallel streaming pipes.

For dry-run mode: annotate the script with `# Parallel group N` comments showing what would run concurrently.

### 7. LiveExecutor Changes

**File:** Modify `LiveExecutor.swift`

- Remove `prefetchedData` pattern (replaced by streaming)
- `executeInTransaction` handles DDL-only steps
- New `executeDataTransfers` method delegates to `ParallelDataTransfer`
- Transaction script no longer includes inline COPY data

## Implementation Order

1. **ConnectionPool** — foundation for everything else
2. **ShellRunner.runPipe** — streaming COPY, independent of pool
3. **Batched introspection** — uses pool
4. **ParallelDataTransfer** — uses pool + streaming
5. **CLI flag + CloneJob** — wire up concurrency setting
6. **Orchestrator integration** — bring it all together
7. **Tests** — unit + integration

## Testing Strategy

- **Unit tests:** ParallelDataTransfer scheduling logic (mock shell), connection pool lifecycle
- **Integration tests:** Clone with `--parallel 2` on partitioned table, verify data integrity
- **Existing tests:** Must continue passing (parallel=1 is equivalent to current sequential)

## Files Changed

| File | Change |
|------|--------|
| `PostgresConnectionPool.swift` | NEW — connection pool |
| `ParallelDataTransfer.swift` | NEW — parallel execution engine |
| `ShellRunner.swift` | ADD `runPipe` method |
| `PGCatalogIntrospector.swift` | Batch queries via TaskGroup |
| `CloneOrchestrator.swift` | Split DDL/data phases |
| `LiveExecutor.swift` | Streaming COPY, parallel data |
| `CloneJob.swift` | Add `parallel` field |
| `CloneCommand.swift` | Add `--parallel` flag |
| `ConfigLoader.swift` | Support `parallel` in YAML |
| `DependencyResolver.swift` | Expose dependency graph for parallel scheduling |
