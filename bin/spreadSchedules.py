#!/usr/bin/python

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler


def str_list_to_int_list(comma_separated_string: str) -> [int]:
    try:
        return list(map(int, comma_separated_string.split(',')))
    except ValueError:
        print('ERROR: Cannot convert list of strings to list of integers')
        exit(1)


def main():
    parser = argparse.ArgumentParser(description='Spread Cicada schedules accross all active servers', add_help=True)
    parser.add_argument("--commit", default=False, action="store_true", help="Commits the change to Cicada, otherwise only print output")
    parser.add_argument('--fromNodes', type=str, help='Optional list of source server ids to collect schedules from')
    parser.add_argument('--toNodes', type=str, help='Optional list of target server id to spread schedules to')
    args = parser.parse_args()

    dbCicada = libPgSQL.init_db()
    from_nodes = str_list_to_int_list(args.fromNodes) if args.fromNodes else None
    to_nodes = str_list_to_int_list(args.toNodes) if args.toNodes else None

    objEnabledServers = libScheduler.getEnabledServers(dbCicada, to_nodes)
    enabledServerCount = len(objEnabledServers)

    if enabledServerCount == 0:
        if from_nodes:
            print("ERROR: Cannot find enabled target server in the provided --toNodes list")
        else:
            print("ERROR: Cannot find enabled target server")
        exit(1)

    nextEnabledServer = 0

    # print(objEnabledServers[1])

    objSchedulesLoadYesterday = libScheduler.getSchedulesLoadYesterday(dbCicada, from_nodes)

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
