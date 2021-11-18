#!/usr/bin/python

import os
import sys
import datetime

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler

from utils import named_exception_handler


@named_exception_handler('showSchedules')
def main():
    dbCicada = libPgSQL.init_db()
    serverId = libPgSQL.getServerId(dbCicada)

    # Get all Asynchronous Schedules
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 1)

    print("")
    print("serverId : " + serverId)
    print("now : " + str(datetime.datetime.now()))
    print("nowMinute : " + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'))
    print("-------------------------------------------------------------------------------------------")
    print("Asynchronous Schedules :")
    print("scheduleId\tFull Command")

    for scheduleId in objSchedules:
        objScheduleDetails = libScheduler.getScheduleExecutable(dbCicada, scheduleId)
        for dRow in objScheduleDetails.fetchall():
            command = str(dRow[0])
            parameters = str(dRow[1])
            print(scheduleId + "\t\t" + command + " " + parameters)

    # Get all Synchronous Schedules
    objSchedules = libScheduler.getAllSchedules(dbCicada, serverId, 0)

    print("")
    print("-------------------------------------------------------------------------------------------")
    print("Synchronous Schedules :")
    print("scheduleId\tFull Command")

    for scheduleId in objSchedules:
        objScheduleDetails = libScheduler.getScheduleExecutable(dbCicada, scheduleId)
        for dRow in objScheduleDetails.fetchall():
            command = str(dRow[0])
            parameters = str(dRow[1])
            print(scheduleId + "\t\t" + command + " " + parameters)

    print("")

    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
