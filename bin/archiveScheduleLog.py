#!/usr/bin/python

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL

def main():
    parser = argparse.ArgumentParser(description='Archive entries older than today from the schedule_log TABLE', add_help=True)

    dbCicada = libPgSQL.init_db()

    sqlquery = """/* Cicada libScheduler */
    START TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    INSERT INTO public.schedule_log_historical
    SELECT * FROM public.schedule_log sl
    WHERE sl.start_time < CURRENT_DATE
    AND end_time is not NULL;

    DELETE FROM public.schedule_log sl
    WHERE sl.start_time < CURRENT_DATE
    AND end_time is not NULL;

    COMMIT TRANSACTION;
    """
    dbCicada.execute(sqlquery)

    libPgSQL.close_db(dbCicada)

if __name__ == "__main__":
    main()