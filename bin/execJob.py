#!/usr/bin/python

import os
import sys
import subprocess
import optparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler


def main():
    usage = "Script usage: %prog scheduleId"
    parser = optparse.OptionParser(usage=usage)
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        exit(101)

    # If parameters are ok, run script
    scheduleId = sys.argv[1]

    dbCicada = libPgSQL.init_db()
    serverId = libPgSQL.getServerId(dbCicada)

    # Get schedule details and execute
    objScheduleDetails = libScheduler.getScheduleDetails(dbCicada, scheduleId)

    for row in objScheduleDetails.fetchall():
        intervalMask = str(row[0])
        command = str(row[1])
        parameters = str(row[2])

        fullCommand = []
        fullCommand.append(command)
        fullCommand.extend(parameters.split())

        # Check to see that schedule is not already running
        if libScheduler.isScheduleRunning(dbCicada, scheduleId) == 0:
            libScheduler.resetAdhocDetails(dbCicada, scheduleId)
            
            # Initiate schedule log
            scheduleLogId = libScheduler.initScheduleLog(dbCicada, serverId, scheduleId)

            # Execute schedule and wait for result
            error_detail = ""
            try:
                returncode = subprocess.check_call(fullCommand)
            except OSError:
                returncode = sys.exc_info()[1][0]
                error_detail = sys.exc_info()[1][1]
            except subprocess.CalledProcessError as e:
                returncode = e.returncode
                error_detail = e.output

            # Finalize schedule log
            libScheduler.finalizeScheduleLog(dbCicada, scheduleLogId, returncode, error_detail)

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
