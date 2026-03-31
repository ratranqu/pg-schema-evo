# CLAUDE.md

## Project Overview

pg-schema-evo is a Swift 6.2 CLI tool for PostgreSQL schema evolution — cloning, syncing, diffing, and inspecting database schemas. Built with Swift Package Manager.

## Prerequisites

- **Swift 6.2** — the project requires Swift 6.2 toolchain
- **Docker** — required for running integration tests (PostgreSQL services)

## Build & Test

```bash
# Build
swift build

# Unit tests only
swift test --filter PGSchemaEvoCoreTests

# Integration tests (requires docker compose up first)
docker compose -f docker/docker-compose.yml up -d
swift test --filter PGSchemaEvoIntegrationTests
docker compose -f docker/docker-compose.yml down

# All tests with coverage
swift test --enable-code-coverage
```

## Code Coverage

Coverage is measured via LLVM/LCOV. The CI merges unit and integration coverage reports.

- **Minimum threshold:** 70%
- **Maximum regression per PR:** 1%
- **Current baseline:** see README.md Status section

To check coverage locally:
```bash
swift test --enable-code-coverage
BIN=$(swift build --show-bin-path)
PROFDATA=$(find .build -name 'default.profdata' -print -quit)
TEST_BIN=$(find "$BIN" -name '*.xctest' -print -quit || find "$BIN" -name 'pg-schema-evoPackageTests' -print -quit)
llvm-cov report -instr-profile="$PROFDATA" "$TEST_BIN" -ignore-filename-regex='Tests/|\.build/'
```

## Project Structure

- `Sources/PGSchemaEvoCLI/` — CLI entry point and commands
- `Sources/PGSchemaEvoCore/` — Core library (introspection, diffing, SQL generation, config)
- `Tests/PGSchemaEvoCoreTests/` — Unit tests (no database required)
- `Tests/PGSchemaEvoIntegrationTests/` — Integration tests (requires PostgreSQL)
- `docker/` — Docker compose and seed scripts for test databases
- `.github/workflows/ci.yml` — CI pipeline (build, test, coverage, docker)

## Roadmap

The roadmap / status is tracked in the `## Status` section of README.md. Update that section when features are added or milestones change.

## Required Workflows

### Before Starting Work

Before starting any work on a branch, rebase from the main branch to ensure you are working on the latest code:

```bash
git fetch origin main
git rebase origin/main
```

### Before Every Commit

Before creating any git commit, you MUST complete ALL of the following steps in order:

1. **Run unit tests** — `swift test --filter PGSchemaEvoCoreTests` — all must pass.
2. **Run integration tests** — Start the test databases with `docker compose -f docker/docker-compose.yml up -d`, run `swift test --filter PGSchemaEvoIntegrationTests`, then `docker compose -f docker/docker-compose.yml down`. All must pass.
3. **Check code coverage** — Run tests with `--enable-code-coverage` and verify:
   - Coverage does NOT fall below **70%**.
   - Coverage does NOT regress by more than **1%** from the baseline in README.md.
   If either threshold is violated, fix the issue before committing.
4. **Update the roadmap** — Update the `## Status` section in README.md to reflect any new features, changes, or milestone progress introduced by the commit.

Do NOT commit if any test fails or coverage thresholds are violated.

### After Every Pull Request

After creating a pull request, you MUST:

1. **Monitor GitHub Actions** — Check that ALL CI workflow jobs pass (build, unit-tests, integration-tests, coverage, docker-build).
2. If any job fails, investigate the failure, fix the issue, push the fix, and verify the CI passes.
3. Do not consider the PR ready until all GitHub Actions checks are green.
