# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Cicada** is a centralized, distributed job scheduler for Pipelinewise taps. It acts as a lightweight management layer between Linux CRON and executables, allowing jobs to be scheduled across multiple nodes via a central database rather than local cron.

Key architectural concepts:
- **Nodes/Servers**: Machines that register with Cicada and pull scheduling information from the central database. They execute `cicada exec_server_schedules` via cron.
- **Schedules**: Jobs defined in the database with cron expressions, parameters, and target servers.
- **SmartScheduling**: A Genetic Algorithm (GA) optimization module that shifts job start times to distribute load across a 24-hour period, avoiding resource conflicts.

## Development Setup

### Install and Build
```bash
make dev          # Create venv with dev dependencies (black, flake8, pytest)
make              # Create venv with only production dependencies
```

The project uses a standard Python venv setup. The `Makefile` is the single source of truth for build commands.

### Run Tests
```bash
make pytest       # Run all tests with coverage (must be ≥80%)
```

To run a single test file or specific test:
```bash
. venv/bin/activate
pytest tests/test_lib_scheduler.py -v
pytest tests/test_lib_scheduler.py::test_function_name -v
```

### Code Quality
```bash
make flake8       # Lint (checks E9, F63, F7, F82 only, max line length 120)
make black        # Format check (line length 120)
```

Black is used for code style; run it with `black --line-length 120 cicada/ tests/ --diff` to preview changes before committing.

## Codebase Structure

### Core Modules

**`cicada/lib/scheduler.py`**
- Central scheduling logic: retrieving schedules, managing execution state, cron parsing
- Functions like `get_schedule_details()`, `get_all_schedule_ids_per_server()`, `get_server_id()`
- Uses `croniter` for cron expression parsing
- Contains SQL queries for the main `schedules` and `servers` tables

**`cicada/lib/postgres.py`**
- Database connection management and helpers
- Connection pooling and statement execution

**`cicada/lib/utils.py`**
- Utility functions and decorators for exception handling and logging

**`cicada/cli.py`**
- Command dispatcher using argparse
- Routes subcommands to handlers in `cicada/commands/`

### Commands
Commands are located in `cicada/commands/` and implement specific operations:
- `exec_server_schedules.py` – Main loop executed by cron on each node; fetches and runs scheduled jobs
- `upsert_schedule.py`, `show_schedule.py`, `delete_schedule.py` – CRUD operations on schedules
- `smart_schedule.py` – Invokes GA optimization (see SmartScheduling below)
- `spread_schedules.py` – Distributes schedules across servers
- `rollback.py` – Reverts SmartScheduling changes using checkpoint history
- `register_server.py`, `archive_schedule_log.py`, `ping_slack.py` – Administrative operations

### SmartScheduling Module
Located in `cicada/lib/SmartScheduling/`

**`domain.py`**
- `Tap` dataclass: represents a schedule as a "tap" (job) with properties:
  - `schedule_id`, `server_id`, `interval_mask` (cron expression)
  - `frequency_minutes`, `median_runtime_minutes`, `cpu_max`
  - `shift`: offset in minutes applied to shift job start time
  - `start_time_mins`: job start time from midnight (calculated from cron)
  - `blacklisted`: flag to exclude from GA optimization

**`config.py`**
- `GAConfig` dataclass: hyperparameters for the genetic algorithm
  - `num_generations`, `sol_per_pop`, `mutation_percent_genes`, etc.
  - `blacklist_schedule_ids`: list of schedule IDs to exclude from optimization

**`pygad.py`**
- Wraps the external `pygad` library (genetic algorithm)
- Fitness function: evaluates how well a shift assignment distributes load
- Implements crossover and mutation operations on shifts

**`evaluation.py`**
- Scoring logic: calculates resource contention, overlap penalties, and fitness metrics

### Database Schema
Key tables:
- `servers` – Registered nodes with hostname, FQDN, IP address
- `schedules` – Job definitions with cron expressions, parameters, execution state
- `schedule_logs` – Historical execution records with runtime, status, output
- `schedule_backups` – GA optimization snapshots for rollback

Database setup SQL is in `setup/db_and_user.sql` and `setup/schema.sql`. Example tap setup for smart scheduling in `setup/create_test_tap_sertup`.

## Key Architectural Patterns

### Cron Expression Handling
- All scheduling uses standard cron format (5 fields: minute hour dom month dow)
- `croniter` library parses expressions and calculates next/previous execution times

### Command Execution
- Jobs are executed as shell commands by `exec_server_schedules`
- Commands can include parameters via template substitution
- Outputs and exit codes are logged to `schedule_logs` table

### Configuration
- Database connection details from `config/definitions.yml` (user must create from `config/example.yml`)
- Each command may accept CLI flags (e.g., `--schedule_id`, `--adhoc_execute`)

### SmartScheduling Workflow
1. **Load schedules**: Fetch all schedules for a server via `get_schedules_per_server()`
2. **Create Tap objects**: Convert schedule details to Tap instances; filter unsupported schedules (irregular cron, too frequent, blacklisted)
3. **Run GA optimization**: PyGAD evolves shifts over N generations to minimize resource conflicts
4. **Apply and checkpoint**: Save optimized shifts back to DB; record checkpoint for potential rollback

## Testing

Tests are in `tests/` and use `pytest` with fixtures:
- `test_functional_main.py` – Integration tests for the main execution loop
- `test_functional_cli_entrypoint.py` – CLI command tests
- `test_functional_spread_schedules.py` – SmartScheduling and load distribution tests
- `test_lib_scheduler.py` – Unit tests for scheduler utility functions
- `test_lib_postgres.py` – Database connection tests

Mock fixtures often include a test PostgreSQL database or in-memory alternatives. Freezegun is used for time-based testing.

## Common Development Tasks

### Adding a New CLI Command
- Create a new file in `cicada/commands/` with a `main()` function
- Import and add an entry point in `cicada/cli.py`
- Add tests in `tests/test_functional_cli_entrypoint.py`

### Modifying Schedule Logic
- Edit `cicada/lib/scheduler.py` for core logic changes (e.g., new state transitions)
- Update `cicada/lib/SmartScheduling/domain.py` if Tap validation rules change
- Update tests in `test_lib_scheduler.py` to cover new behavior

### Database Schema Changes
- Modify SQL in `setup/schema.sql` (note: existing deployments require migration scripts)
- Update query strings in `scheduler.py` and corresponding test fixtures

## Important Notes

- **PostgreSQL only**: Only PostgreSQL is supported (versions 12.9–15.14 verified)
- **No external APIs**: Uses only core Python and database; runs offline
- **Cron safety**: Jobs execute only when registered server node is running; they respect cron expressions and database state
- **Rollback support**: SmartScheduling changes can be rolled back via checkpoints stored in the database
- **Line length**: Maximum 120 characters (enforced by Black and Flake8)
- **Code coverage**: Must maintain ≥80% test coverage for commits
