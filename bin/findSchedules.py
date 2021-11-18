#!/usr/bin/python

import os
import sys
import subprocess

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../bin"))

import libPgSQL
import libScheduler

from utils import named_exception_handler


@named_exception_handler('findSchedules')
def main():
    dbCicada = libPgSQL.init_db()
    serverId = libPgSQL.getServerId(dbCicada)

    # Get all schedules and execute asynchronously
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 1)

    for scheduleId in objSchedules:
        fullCommand = [
            sys.executable,
            os.path.abspath(os.path.dirname(sys.argv[0])) + '/execSchedule.py',
            str(scheduleId)
        ]

        with open(os.devnull, 'w') as devnull:
            # subprocess.Popen = asyncronous
            subprocess.Popen(fullCommand, stdout=devnull)

    # Get all schedules and execute synchronously
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 0)

    for scheduleId in objSchedules:
        fullCommand = [
            sys.executable,
            os.path.abspath(os.path.dirname(sys.argv[0])) + '/execSchedule.py',
            str(scheduleId)
        ]

        with open(os.devnull, 'w') as devnull:
            # subprocess.call = syncronous
            subprocess.call(fullCommand, stdout=devnull)

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
