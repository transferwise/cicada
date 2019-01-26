#!/usr/bin/python

import os
import sys
import subprocess

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler


def main():
    dbCicada = libPgSQL.init_db()
    serverId = libPgSQL.getServerId(dbCicada)

    # Get all schedules and execute asynchronously
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 1)

    for row in objSchedules:
        scheduleId = str(row[0])

        print(sys.executable)

        fullCommand = []
        fullCommand.append(sys.executable)
        fullCommand.append(os.path.abspath(os.path.dirname(sys.argv[0])) + '/execJob.py')
        fullCommand.append(str(scheduleId))

        with open(os.devnull, 'w') as devnull:
            # subprocess.Popen = asyncronous
            subprocess.Popen(fullCommand, stdout=devnull)

    # Get all schedules and execute synchronously
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 0)

    for row in objSchedules:
        scheduleId = str(row[0])

        fullCommand = []
        fullCommand.append(sys.executable)
        fullCommand.append(os.path.abspath(os.path.dirname(sys.argv[0])) + '/execJob.py')
        fullCommand.append(str(scheduleId))

        with open(os.devnull, 'w') as devnull:
            # subprocess.call = syncronous
            subprocess.call(fullCommand, stdout=devnull)

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
