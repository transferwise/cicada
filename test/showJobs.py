#!/usr/bin/python

import os
import sys
import datetime

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler


def main():
    dbCicada = libPgSQL.init_db()
    serverId = libPgSQL.getServerId(dbCicada)

    # Get all Asynchronous Schedules
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 1)

    print("===========================================================================================")
    print("now\t\t: " + str(datetime.datetime.now()))
    print("nowMinute\t: " + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'))
    print("===========================================================================================")
    print("scheduleId\tintervalMask\t\tadhoc\tCommand\t\t\tParameters")
    print("-------------------------------------------------------------------------------------------")
    print("-- Asynchronous Schedules -----------------------------------------------------------------")
    print("-------------------------------------------------------------------------------------------")

    for sRow in objSchedules:
        scheduleId = str(sRow[0])

        objScheduleDetails = libScheduler.getScheduleDetails(dbCicada, scheduleId)
        for dRow in objScheduleDetails.fetchall():
            intervalMask = str(dRow[0])
            command = str(dRow[1])
            parameters = str(dRow[2])

            print(scheduleId + "\t\t" + intervalMask + "\t" + command + "\t\t" + parameters)

    # Get all Synchronous Schedules
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 0)

    print("")
    print("-------------------------------------------------------------------------------------------")
    print("-- Synchronous Schedules ------------------------------------------------------------------")
    print("-------------------------------------------------------------------------------------------")

    for sRow in objSchedules:
        scheduleId = str(sRow[0])

        objScheduleDetails = libScheduler.getScheduleDetails(dbCicada, scheduleId)
        for dRow in objScheduleDetails.fetchall():
            intervalMask = str(dRow[0])
            command = str(dRow[1])
            parameters = str(dRow[2])

            print(scheduleId + "\t\t" + intervalMask + "\t" + command + "\t\t" + parameters)

    print("===========================================================================================")
    print("")

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
