#!/usr/bin/python

import os
import sys
import subprocess

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler


def main():
    dbCicada = libPgSQL.init_db()

    objEnabledServers = libScheduler.getEnabledServers(dbCicada)
    enabledServerCount = len(objEnabledServers)
    nextEnabledServer = 0

    # print(objEnabledServers[1])

    objSchedulesLoadYesterday = libScheduler.getSchedulesLoadYesterday(dbCicada)

    for scheduleId in objSchedulesLoadYesterday:
        if nextEnabledServer == enabledServerCount:
            nextEnabledServer = 0
        print(nextEnabledServer)
        # objScheduleDetails = libScheduler.getScheduleDetails(dbCicada, scheduleId)


    libPgSQL.close_db(dbCicada)


if __name__ == "__main__":
    main()
