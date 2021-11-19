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

    CREATE TEMP TABLE running_log_entry (
        schedule_id character varying(255) NOT NULL,
        start_time timestamp(3) without time zone NOT NULL,
        CONSTRAINT running_log_entry_pkey PRIMARY KEY (schedule_id)
    );

    INSERT INTO running_log_entry
    SELECT
        sl.schedule_id,
        MAX(sl.start_time)
    FROM schedule_log sl
        INNER JOIN schedules s USING (schedule_id)
    WHERE sl.end_time IS null
        AND s.is_running = 1
    GROUP BY schedule_id
    ;

    CREATE TEMP TABLE archivable_log_entries (
        schedule_log_id character varying(64) NOT NULL,
        CONSTRAINT archivable_log_entries_pkey PRIMARY KEY (schedule_log_id)
    );

    INSERT INTO archivable_log_entries
    SELECT
        sl.schedule_log_id
    FROM schedule_log sl
        LEFT JOIN running_log_entry rle USING (schedule_id, start_time)
    WHERE rle.start_time IS null
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

    COMMIT TRANSACTION;
    """.format(daysToKeep)
    dbCicada.execute(sqlquery)

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
