# Repository Guidance

## Scope

Cicada is a Python 3.8, PostgreSQL-backed distributed job scheduler. Keep changes small, preserve command-line and
database compatibility, and treat the implementation and `setup/schema.sql` as authoritative when documentation is
stale. Explicit user instructions override this file.

`AGENTS.md` is the authoritative repository guidance; keep `CLAUDE.md` as a symlink to it.

## Architecture

- `cicada/cli.py` owns argparse and dispatches commands to `cicada/commands/`.
- `cicada/lib/scheduler.py` owns schedule selection, cron evaluation, database queries, snapshots, and rollback helpers.
- `cicada/commands/exec_server_schedules.py` is the cron entry point. It starts asynchronous schedules with `Popen`,
  then runs synchronous schedules sequentially.
- `cicada/commands/exec_schedule.py` owns one execution's state, log, subprocess, abort, and cleanup lifecycle.
- `cicada/lib/smart_scheduling/` contains the optimizer domain, configuration, evaluation, and PyGAD adapter.
- `setup/schema.sql` defines fresh databases; `setup/`, `reports/`, `docs/`, and `local-dev/` contain operational SQL,
  reports, documentation, and the supported development stack.

## Execution Invariants

- The database coordinates execution through `is_running`, `abort_running`, `adhoc_execute`, and `adhoc_parameters`.
  Preserve their one-shot behavior and update related state atomically where races are possible.
- Normal schedules require enabled schedule and server records and valid date bounds. Ad hoc execution bypasses the
  schedule's enabled/date filters but still requires an enabled server and `is_running = 0`.
- Never clear `is_running` while a launched child may still be alive. An abort or handled supervisor shutdown sends
  `SIGTERM` only to the direct child, waits for it to exit, and consumes repeated abort requests while waiting. Cicada
  does not manage descendants or escalate signals; launchers must remain foreground and `exec` or correctly forward
  signals and wait for their work.
- Commands are converted to argv with `shlex.split` and launched without a shell. Preserve quoting behavior and do not
  introduce `shell=True`. Child stdout and stderr are discarded; `schedule_log` stores metadata and status, not output.
- PostgreSQL connections require SSL and use autocommit. Wrap multi-statement mutations in explicit
  `BEGIN`/`COMMIT`/`ROLLBACK`, and prefer parameterized SQL for new or changed queries.
- The effective cron is `COALESCE(smart_interval_mask, interval_mask)`. Smart scheduling must leave disabled,
  irregular, and blocklisted schedules unchanged, preserve the original interval, and retain five snapshots per server.

## Environment and Validation

Use the Make targets as the canonical commands:

```bash
make dev
make flake8
make black
make pytest
```

- `make flake8` checks fatal rules in `cicada/`; `make black` checks `cicada/` and `tests/` at 120 columns.
- `make pytest` runs `tests/` with coverage and requires at least 78%.
- Prefer the `local-dev` Docker stack for verification, tests, and linting; it supplies Python 3.8, PostgreSQL, TLS,
  schema, and the supported environment. Reuse a ready `cicada_dev` container when one is already running:

```bash
docker compose -f local-dev/docker-compose.yml up -d --build  # when the stack is not ready
docker logs cicada_dev  # wait for "Cicada Dev environment is ready"
docker exec cicada_dev make flake8
docker exec cicada_dev make black
docker exec cicada_dev make pytest
```

- The first container start is slow because it installs system and Python dependencies. Do not confuse startup with a
  test failure.
- Leave the local development containers running after verification. Stop them only when explicitly requested or when
  a clean rebuild is required.
- Do not share `venv/` between the host and container; rebuild it in the environment that will execute the checks.
- Database-backed test modules create and drop databases through ordered tests. Run the files serially; do not reorder
  them, use `-x`, or run a later test without its module's setup test.
- Never run `local-dev/refresh-local-dev.sh` without explicit approval; it performs a global Docker prune and removes
  the development virtual environment with elevated privileges.
- Add focused tests for changed behavior, then run the broadest relevant Make target. Report exact pass, fail, and skip
  counts; an unavailable or skipped check is not a pass.
- Summarize test changes as **Existing**, **New**, **Updated**, and **Deleted** test cases. Separate coverage of the
  changed behavior from broader regression or supporting coverage, and briefly explain what each updated case now
  verifies. Report test cases, not test-function counts. Include this summary in every PR description, using **None**
  for categories without test changes.

## Schema, Configuration, and Releases

- For schema changes, update `setup/schema.sql` for fresh installs and add an idempotent, transactional migration for
  existing databases when required. There is no migration runner or version ledger, so deploy and verify upgrade SQL
  explicitly. Update fixtures and query code together.
- Treat `setup/schema.sql` as the source of truth for `docs/erd.excalidraw`. After every schema change, update the
  editable scene and export `docs/erd.png` for README and GitHub rendering. Show all tables and declared PKs/FKs; use
  right-angled FK lines labelled `*`, `1`, `1:1`, or `0..1`, dashed when nullable and solid when required.
- Runtime configuration is loaded from ignored `config/definitions.yml`; `config/example.yml` documents its shape.
  Do not print, commit, or overwrite populated credentials or environment files.
- Add CLI commands in `cicada/commands/`, wire them through `cicada/cli.py`, and cover dispatch plus command behavior.
- Update `README.md` or `docs/` for user-visible behavior and `CHANGELOG.md` for release-visible changes. Change the
  package version in `setup.py` only as part of a release, keeping it aligned with the changelog and release tag.
- Keep CHANGELOG entries concise and atomic: one independently reviewable change per bullet. Use plain operational
  language that says what users or operators will observe and why it matters; avoid implementation jargon unless it
  helps them act or diagnose a problem. Use headings to keep categories separate.
- Write documentation in a concise, authoritative, pragmatic, mildly operations-first engineering tone. Avoid
  repetition except where it prevents operational mistakes.

## Working Discipline

- Preserve unrelated working-tree changes and avoid broad formatting or opportunistic refactors.
- Keep comments focused on non-obvious constraints or consequences; do not narrate the code.
- Before completion, run applicable tests and lint, run `git diff --check`, and verify `git status` contains only
  intended files.
