#!/usr/bin/python

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../bin"))
import libPgSQL

from utils import named_exception_handler


@named_exception_handler('archiveSchedules')
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

    INSERT INTO public.schedule_log_historical
    SELECT * FROM public.schedule_log sl
    WHERE sl.start_time < CURRENT_DATE +1 -{}
    AND end_time is not NULL;

    DELETE FROM public.schedule_log sl
    WHERE sl.start_time < CURRENT_DATE +1 -{}
    AND end_time is not NULL;

    COMMIT TRANSACTION;
    """.format(daysToKeep, daysToKeep)
    dbCicada.execute(sqlquery)

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
