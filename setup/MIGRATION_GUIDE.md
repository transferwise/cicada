# Schema Migration Guide

This guide explains how to add schema changes that work gracefully for both fresh installations and existing deployments.

## Philosophy

Cicada's migration strategy ensures:
- **Fresh installs** get the complete schema from `schema.sql`
- **Existing installs** are updated via migration scripts
- **Idempotency** — migrations can be run multiple times safely
- **Rollback awareness** — changes integrate with the existing `schedule_changes` audit trail

## Pattern: Adding a New Column to `schedules`

This is the most common migration scenario. Here's the approach:

### Step 1: Update `setup/schema.sql`

Add the column definition **within the `CREATE TABLE IF NOT EXISTS public.schedules` statement**:

```sql
-- In the CREATE TABLE IF NOT EXISTS public.schedules section:
CREATE TABLE IF NOT EXISTS public.schedules
(
  -- ... existing columns ...
  my_new_column VARCHAR(255) DEFAULT 'default_value',  -- Add here
  CONSTRAINT schedules_pkey PRIMARY KEY (schedule_id),
  -- ... rest of constraints ...
)
WITH (OIDS=FALSE);
```

Then, add a defensive `ALTER TABLE` with `IF NOT EXISTS` immediately after the `CREATE TABLE`:

```sql
-- Add my_new_column if not exists (for existing installations upgrading)
ALTER TABLE public.schedules
ADD COLUMN IF NOT EXISTS my_new_column VARCHAR(255) DEFAULT 'default_value';
```

**Why both?**
- The column in `CREATE TABLE` satisfies fresh installs (cleaner schema)
- The `ALTER TABLE ... IF NOT EXISTS` ensures existing installations get the column without errors

### Step 2: Create a Versioned Migration Script (Optional but Recommended)

If the migration is complex or you want a separate, self-contained migration file:

Create `setup/migrate_YYYYMMDD_description.sql`:

```sql
/** Migration: Add my_new_column to schedules
    Run as cicada user on db_cicada database
    Date: 2026-05-21
**/
START TRANSACTION;

-- Add my_new_column to schedules if it doesn't exist
ALTER TABLE public.schedules
ADD COLUMN IF NOT EXISTS my_new_column VARCHAR(255) DEFAULT 'default_value';

-- If the migration adds a column with NOT NULL and no default,
-- backfill existing rows first:
-- UPDATE public.schedules SET my_new_column = 'backfill_value' 
--   WHERE my_new_column IS NULL;

COMMIT TRANSACTION;
```

**When to create a separate migration:**
- The change requires data backfilling
- The change is complex (e.g., adding constraints, creating indexes, altering types)
- You want an audit trail of when the migration was run
- The migration takes significant time (separate script = easier to monitor)

**Naming convention:** `migrate_YYYYMMDD_short_description.sql` (e.g., `migrate_20260521_add_priority_column.sql`)

### Step 3: Update Code to Handle Missing Columns

In `cicada/lib/scheduler.py` and related modules, make code defensive:

```python
# Instead of assuming the column exists:
# schedule = result['my_new_column']  # ❌ KeyError on old schema

# Use getattr with defaults:
schedule = result.get('my_new_column', 'default_value')  # ✅ Safe for old and new schema

# Or use SQL COALESCE for consistent defaults:
SELECT 
  *,
  COALESCE(my_new_column, 'default_value') as my_new_column
FROM schedules;
```

### Step 4: Update Tests

Add fixtures that test **both old and new schema** versions:

```python
# tests/conftest.py or relevant test file

@pytest.fixture
def db_with_old_schema(pg_conn):
    """Database with old schema (before migration)"""
    # Run schema.sql without the new column
    # Or mock a result without the column
    return pg_conn

@pytest.fixture
def db_with_new_schema(pg_conn):
    """Database with new schema (after migration)"""
    # Run full schema.sql with the new column
    return pg_conn

def test_code_handles_missing_column(db_with_old_schema):
    # Verify code doesn't crash when column is missing
    result = get_schedule_details(schedule_id, db=db_with_old_schema)
    assert result['schedule_id'] == schedule_id
    # Should not raise KeyError even though my_new_column is missing
```

## Pattern: Creating a New Table

### Example: Adding `schedule_notifications` table

#### Step 1: Update `schema.sql`

```sql
-- Table: schedule_notifications
-- New table to track notification settings for schedules
CREATE TABLE IF NOT EXISTS public.schedule_notifications
(
  notification_id SERIAL NOT NULL,
  schedule_id VARCHAR(255) NOT NULL,
  notify_on_failure SMALLINT NOT NULL DEFAULT 1,
  notify_email VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT schedule_notifications_pkey PRIMARY KEY (notification_id),
  CONSTRAINT schedule_notifications_schedule_fkey FOREIGN KEY (schedule_id)
      REFERENCES public.schedules (schedule_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (OIDS=FALSE);

CREATE INDEX IF NOT EXISTS schedule_notifications_schedule_id_idx
  ON public.schedule_notifications
  USING btree (schedule_id);
```

#### Step 2: Create Migration Script (Recommended)

```sql
/** Migration: Add schedule_notifications table
    Run as cicada user on db_cicada database
    Date: 2026-05-21
**/
START TRANSACTION;

CREATE TABLE IF NOT EXISTS public.schedule_notifications
(
  notification_id SERIAL NOT NULL,
  schedule_id VARCHAR(255) NOT NULL,
  notify_on_failure SMALLINT NOT NULL DEFAULT 1,
  notify_email VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT schedule_notifications_pkey PRIMARY KEY (notification_id),
  CONSTRAINT schedule_notifications_schedule_fkey FOREIGN KEY (schedule_id)
      REFERENCES public.schedules (schedule_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (OIDS=FALSE);

CREATE INDEX IF NOT EXISTS schedule_notifications_schedule_id_idx
  ON public.schedule_notifications
  USING btree (schedule_id);

COMMIT TRANSACTION;
```

## Pattern: Modifying an Existing Column

### Example: Making `parameters` column larger

#### Step 1: Update `schema.sql`

Document the change in a comment:

```sql
-- Modified 2026-05-21: Increased from VARCHAR(1024) to VARCHAR(4096)
parameters VARCHAR(4096),
```

#### Step 2: Create Migration Script

```sql
/** Migration: Increase parameters column size
    Run as cicada user on db_cicada database
    Date: 2026-05-21
**/
START TRANSACTION;

-- Alter the column type
ALTER TABLE public.schedules
ALTER COLUMN parameters TYPE VARCHAR(4096);

COMMIT TRANSACTION;
```

**Note:** Expanding VARCHAR is always safe and fast in PostgreSQL. Contracting requires data validation first.

## Pattern: Adding an Index

### Example: Optimize queries on `interval_mask`

#### Step 1: Update `schema.sql`

```sql
-- Index: schedules_interval_mask_idx
CREATE INDEX IF NOT EXISTS schedules_interval_mask_idx
  ON public.schedules
  USING btree (interval_mask);
```

#### Step 2: Create Migration Script

```sql
/** Migration: Add index on schedules.interval_mask
    Run as cicada user on db_cicada database
    Date: 2026-05-21
**/
START TRANSACTION;

CREATE INDEX IF NOT EXISTS schedules_interval_mask_idx
  ON public.schedules
  USING btree (interval_mask);

COMMIT TRANSACTION;
```

**Note:** Use `IF NOT EXISTS` so the migration is idempotent.

## Deployment Workflow

### For Fresh Installations
1. User runs `setup/schema.sql` → gets complete schema with all columns, tables, indexes

### For Existing Installations
1. User deploys new code version
2. User applies migration scripts **in order** by date:
   ```bash
   psql -U cicada -d db_cicada -f setup/migrate_20260501_first_change.sql
   psql -U cicada -d db_cicada -f setup/migrate_20260521_second_change.sql
   ```
3. Code continues running (defensive handling of optional columns/tables)

### Deployment Documentation

Include in your release notes:

```markdown
## v2.0.0 - 2026-05-21

### Database Migrations Required

Run migrations **in order**:
```bash
psql -U cicada -d db_cicada -f setup/migrate_20260501_new_feature.sql
psql -U cicada -d db_cicada -f setup/migrate_20260521_add_priority.sql
```

### Changes
- Added `priority` column to `schedules` table
- Added new `schedule_notifications` table for notification settings
- Code is backward-compatible; migrations are optional but recommended for full feature support

### Rollback
If you encounter issues:
- Migrations can be reversed by dropping the new columns/tables (see comments in migration scripts)
- Older code versions will continue to work with the new schema (via defensive defaults)
```

## Testing Migrations Locally

### Setup
```bash
# Create a test database
psql -U postgres -c "CREATE DATABASE db_cicada_test;"
psql -U postgres -d db_cicada_test -c "CREATE USER cicada WITH PASSWORD 'password';"
psql -U postgres -d db_cicada_test -c "GRANT ALL PRIVILEGES ON DATABASE db_cicada_test TO cicada;"
```

### Test Fresh Install
```bash
psql -U cicada -d db_cicada_test -f setup/schema.sql
```

### Test Migration Path
```bash
# Apply old schema, then run migrations
psql -U cicada -d db_cicada_test -f setup/schema.sql
psql -U cicada -d db_cicada_test -f setup/migrate_20260521_add_priority_column.sql

# Verify the migration
psql -U cicada -d db_cicada_test -c "SELECT column_name FROM information_schema.columns 
  WHERE table_name='schedules' AND column_name='priority';"
```

## Best Practices

1. **Always use `IF NOT EXISTS`** for idempotency
   - `CREATE TABLE IF NOT EXISTS`
   - `CREATE INDEX IF NOT EXISTS`
   - `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`

2. **Set sensible defaults** for new columns
   - Avoid `NULL` unless necessary
   - Use `DEFAULT` clause in schema

3. **Wrap migrations in transactions**
   ```sql
   START TRANSACTION;
   -- migration SQL
   COMMIT TRANSACTION;
   ```

4. **Make code defensive**
   - Use `dict.get()` with defaults instead of direct key access
   - Use SQL `COALESCE()` for optional columns in queries
   - Catch exceptions gracefully if a table doesn't exist yet

5. **Document changes**
   - Add comments in `schema.sql` noting when columns were added/modified
   - Include the date and reason in migration script headers
   - Update `CLAUDE.md` if architectural changes occur

6. **Test both paths**
   - Verify fresh installs work
   - Verify migrations work on existing schema
   - Verify code handles missing columns gracefully

## Rollback Guidance

If a migration causes issues:

### Reversing Column Additions
```sql
ALTER TABLE public.schedules
DROP COLUMN my_new_column;
```

### Reversing Table Creations
```sql
DROP TABLE IF EXISTS public.schedule_notifications CASCADE;
```

### Reversing Index Additions
```sql
DROP INDEX IF EXISTS public.schedule_notifications_schedule_id_idx;
```

Document these in comments within the migration file for quick reference.

## References

- See `setup/schema.sql` for the current complete schema
- See existing patterns like the `smart_interval_mask` column (added in line 96-98)
- See the `schedule_blocklist` table (added with `IF NOT EXISTS` pattern)
