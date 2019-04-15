#!/usr/bin/python
import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL
import libScheduler

def main():
    parser = argparse.ArgumentParser(description='Add or change a schedule in Cicada', add_help=True)
    parser.add_argument('--scheduleId', type=str, required=True, help="Id of the schedule")
    parser.add_argument('--scheduleDescription', type=str, help="Description of schedule")
    parser.add_argument('--serverId', type=int, help="Id of the server where the job will run")
    parser.add_argument('--scheduleOrder', type=int, help="run order for the schedule. lowest is first. is_async jobs will be executed all at once")
    parser.add_argument('--isAsync', type=str, help="0=disabled 1=enabled | is_async jobs execute in parallel")
    parser.add_argument('--isEnabled', type=str, help="0=Disabled 1=Enabled")
    parser.add_argument('--adhocExecute', type=str, help="0=Disabled 1=Enabled | The job will execute at next minute, regardless of other schedule time settings")
    parser.add_argument('--intervalMask', type=str, help="When to execute the command | Modeled on unix crontab (minute hour dom month dow)")
    parser.add_argument('--firstRunDate', type=str, help="The schedule will not execute before this datetime")
    parser.add_argument('--lastRunDate', type=str, help="The schedule will not execute after this datetime")
    parser.add_argument('--command', type=str, help="Command to execute")
    parser.add_argument('--parameters', type=str, help="Exact string of parameters for command")
    parser.add_argument('--adhocParameters', type=str, help="If specified, will override parameters for next run")
    parser.add_argument('--scheduleGroupId', type=str, help="Optional field to help with schedule grouping")

    args = parser.parse_args()
    scheduleId = args.scheduleId

    dbCicada = libPgSQL.init_db()

    # Get schedule details
    objScheduleDetails = libScheduler.getScheduleDetails(dbCicada, scheduleId)
    for row in objScheduleDetails.fetchall():
        scheduleDescription = str(row[0])
        serverId = str(row[1])
        scheduleOrder = str(row[2])
        isAsync = str(row[3])
        isEnabled = str(row[4])
        adhocExecute = str(row[5])
        intervalMask = str(row[6])
        firstRunDate = str(row[7])
        lastRunDate = str(row[8])
        command = str(row[9])
        parameters = str(row[10])
        adhocParameters = str(row[11])
        scheduleGroupId = str(row[12])

    if args.scheduleDescription is not None:
        scheduleDescription = args.scheduleDescription

    if args.scheduleOrder is not None:
        scheduleOrder = args.scheduleOrder

    if args.isAsync is not None:
        isAsync = args.isAsync

    if args.isEnabled is not None:
        isEnabled = args.isEnabled

    if args.adhocExecute is not None:
        adhocExecute = args.adhocExecute

    if args.intervalMask is not None:
        intervalMask = args.intervalMask

    if args.firstRunDate is not None:
        firstRunDate = args.firstRunDate

    if args.lastRunDate is not None:
        lastRunDate = args.lastRunDate

    if args.command is not None:
        command = args.command

    if args.parameters is not None:
        parameters = args.parameters

    if args.adhocParameters is not None:
        adhocParameters = args.adhocParameters

    if args.scheduleGroupId is not None:
        scheduleGroupId = args.scheduleGroupId


    print(scheduleDescription)



if __name__ == "__main__":
    main()
