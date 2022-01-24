"""
Archive entries from schedule_log into schedule_log_historical.

Entries older than --days_to_keep are moved
Entries of running jobs are not moved
"""

import sys

from cicada.lib import postgres
from cicada.lib import utils


@utils.named_exception_handler("archive_schedule_log")
def main(days_to_keep, dbname=None):
    """Archive entries from schedule_log into schedule_log_historical."""

    if not days_to_keep > 0:
        print("--days_to_keep needs to be greater than 0")
        sys.exit(1)

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    sqlquery = f"""
    START TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    CREATE TEMP TABLE valuable_log_entries AS
    SELECT schedule_log_id
    FROM (
        SELECT
            schedule_log_id,
            start_time,
            max(start_time) OVER (PARTITION BY schedule_id) AS max_start_time
        FROM schedule_log sl
            INNER JOIN schedules USING (schedule_id)
        WHERE is_running = 1
        ORDER BY schedule_id
    ) running_log_entries
    WHERE start_time = max_start_time
    UNION
    SELECT schedule_log_id
    FROM (
        SELECT
            schedule_log_id,
            start_time,
            max(start_time) OVER (PARTITION BY schedule_id) AS max_start_time
        FROM schedule_log sl
        WHERE end_time IS NOT null
        ORDER BY schedule_id
    ) completed_log_entries
    WHERE start_time = max_start_time
    ;

    CREATE TEMP TABLE archivable_log_entries AS
    SELECT
        sl.schedule_log_id
    FROM schedule_log sl
        LEFT JOIN valuable_log_entries vle USING (schedule_log_id)
    WHERE vle.schedule_log_id IS null
        AND sl.start_time < CURRENT_DATE +1 -{days_to_keep}
    ;

    INSERT INTO schedule_log_historical
    SELECT schedule_log.*
    FROM schedule_log
    WHERE schedule_log_id IN (SELECT schedule_log_id FROM archivable_log_entries)
    ;

    DELETE FROM schedule_log sl
    WHERE schedule_log_id IN (SELECT schedule_log_id FROM archivable_log_entries)
    ;

    DROP TABLE IF EXISTS valuable_log_entries;
    DROP TABLE IF EXISTS archivable_log_entries;

    COMMIT TRANSACTION;
    """

    db_cur.execute(sqlquery)

    db_conn.close()
