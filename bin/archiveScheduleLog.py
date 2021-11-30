#!/usr/bin/python

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL

from utils import named_exception_handler


@named_exception_handler('archiveScheduleLog')
def main():
    parser = argparse.ArgumentParser(description='Archive completed entries from the schedule_log TABLE', add_help=True)
    parser.add_argument('--daysToKeep', type=int, required=True, help='Amount of days to keep in schedule_log')
    args = parser.parse_args()

    # Execute only if --daysToKeep is greater than 0
    if args.daysToKeep < 1:
        print('--daysToKeep needs to be greater than 0')
        exit(0)

    daysToKeep = str(args.daysToKeep)

    dbCicada = libPgSQL.init_db()

    sqlquery = """/* Cicada archiveScheduleLog */
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
        AND sl.start_time < CURRENT_DATE +1 -{}
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
    """.format(daysToKeep)
    dbCicada.execute(sqlquery)

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
