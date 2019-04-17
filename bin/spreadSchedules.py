#!/usr/bin/python

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler


def main():
    parser = argparse.ArgumentParser(description='Spread Cicada schedules accross all active servers', add_help=True)
    parser.add_argument("--commit", default=False, action="store_true", help="Commits the change to Cicada, otherwise only print output")
    args = parser.parse_args()

    dbCicada = libPgSQL.init_db()

    objEnabledServers = libScheduler.getEnabledServers(dbCicada)
    enabledServerCount = len(objEnabledServers)
    nextEnabledServer = 0

    # print(objEnabledServers[1])

    objSchedulesLoadYesterday = libScheduler.getSchedulesLoadYesterday(dbCicada)

    for scheduleId in objSchedulesLoadYesterday:

        currentScheduleDetails = libScheduler.getScheduleDetails(dbCicada, scheduleId)
        newScheduleDetails = currentScheduleDetails.copy()
        newScheduleDetails['serverId'] = objEnabledServers[nextEnabledServer]

        nextEnabledServer = nextEnabledServer + 1
        if nextEnabledServer == enabledServerCount:
            nextEnabledServer = 0

        if args.commit:
            libScheduler.updateScheduleDetails(dbCicada, newScheduleDetails)
        else:
            print(str("\'" + str(currentScheduleDetails['scheduleId']) +
                "\' will be reassigned : " + str(currentScheduleDetails['serverId']) + " -> " + str(newScheduleDetails['serverId'])))

    libPgSQL.close_db(dbCicada)

if __name__ == "__main__":
    main()
