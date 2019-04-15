#!/usr/bin/python
import os
import sys
import argparse
from tabulate import tabulate

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + '/../lib'))
import libPgSQL
import libScheduler

commands = [
    'show',
    'upsert'
]

def main():
    parser = argparse.ArgumentParser(description='Manage a Cicada schedule', add_help=True)
    parser.add_argument('command', type=str, help=', '.join(commands))
    parser.add_argument('--scheduleId', type=str, required=True, help='Id of the schedule')
    parser.add_argument('--scheduleDescription', type=str, help='Schedule description and comments')
    parser.add_argument('--serverId', type=int, help='Id of the server where the schedule will run')
    parser.add_argument('--scheduleOrder', type=int, help='run order for the schedule. lowest is first. isAsync jobs will be executed all at once')
    parser.add_argument('--isAsync', type=str, help='0=disabled 1=Enabled | isAsync jobs execute in parallel')
    parser.add_argument('--isEnabled', type=str, help='0=Disabled 1=Enabled')
    parser.add_argument('--adhocExecute', type=str, help='0=Disabled 1=Enabled | The job will execute at next minute, regardless of other schedule time settings')
    parser.add_argument('--intervalMask', type=str, help='When to execute the command | Modeled on unix crontab (minute hour dom month dow)')
    parser.add_argument('--firstRunDate', type=str, help='The schedule will not execute before this datetime')
    parser.add_argument('--lastRunDate', type=str, help='The schedule will not execute after this datetime')
    parser.add_argument('--execCommand', type=str, help='Command to execute')
    parser.add_argument('--parameters', type=str, help='Exact string of parameters for command')
    parser.add_argument('--adhocParameters', type=str, help='If specified, will override parameters for next run')
    parser.add_argument('--scheduleGroupId', type=str, help='Optional field to help with schedule grouping')
    args = parser.parse_args()

    # Declare variables
    scheduleId = args.scheduleId
    scheduleDescription = None
    serverId = None
    scheduleOrder = None
    isAsync = None
    isEnabled = None
    adhocExecute = None
    intervalMask = None
    firstRunDate = None
    lastRunDate = None
    execCommand = None
    parameters = None
    adhocParameters = None
    scheduleGroupId = None

    # Get schedule details
    dbCicada = libPgSQL.init_db()
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
        execCommand = str(row[9])
        parameters = str(row[10])
        adhocParameters = str(row[11])
        scheduleGroupId = str(row[12])
        isRunning = str(row[13])


    # Perform "print" command
    if (args.command == 'show'):
        if serverId is not None:
            table = [
            ['Setting', 'Value'],
            ['scheduleId', scheduleId],
            ['scheduleDescription', scheduleDescription],
            ['serverId', serverId],
            ['scheduleOrder', scheduleOrder],
            ['isAsync', isAsync],
            ['isEnabled', isEnabled],
            ['adhocExecute', adhocExecute],
            ['intervalMask', intervalMask],
            ['firstRunDate', firstRunDate],
            ['lastRunDate', lastRunDate],
            ['execCommand', execCommand],
            ['parameters', parameters],
            ['adhocParameters', adhocParameters],
            ['scheduleGroupId', scheduleGroupId],
            ['isRunning', isRunning]
            ]
            print(tabulate(table, headers="firstrow", tablefmt="psql"))
            exit(0)
        else:
            print('ERROR: Schedule \"' + scheduleId + '\" not found')
            exit(1)


    # Perform "upsert" command
    if (args.command == 'upsert'):
        if args.scheduleDescription is not None:
            scheduleDescription = args.scheduleDescription

        if args.serverId is not None:
            serverId = args.serverId
        else:
            if serverId is None:
                serverId = 1

        if args.scheduleOrder is not None:
            scheduleOrder = args.scheduleOrder
        else:
            if scheduleOrder is None:
                scheduleOrder = 1

        if args.isAsync is not None:
            isAsync = args.isAsync
        else:
            if isAsync is None:
                isAsync = 1

        if args.isEnabled is not None:
            isEnabled = args.isEnabled
        else:
            if isEnabled is None:
                isEnabled = 0

        if args.adhocExecute is not None:
            adhocExecute = args.adhocExecute

        if args.intervalMask is not None:
            intervalMask = args.intervalMask
        else:
            if intervalMask is None:
                print('intervalMask cannot be None')
                exit(1)

        if args.firstRunDate is not None:
            firstRunDate = args.firstRunDate
        else:
            if firstRunDate is None:
                firstRunDate = '1000-01-01 00:00:00.000'

        if args.lastRunDate is not None:
            lastRunDate = args.lastRunDate
        else:
            if lastRunDate is None:
                lastRunDate = '9999-12-31 23:59:59.999'

        if args.execCommand is not None:
            execCommand = args.execCommand
        else:
            if execCommand is None:
                print('execCommand cannot be None')
                exit(1)

        if args.parameters is not None:
            parameters = args.parameters

        if args.adhocParameters is not None:
            adhocParameters = args.adhocParameters

        if args.scheduleGroupId is not None:
            scheduleGroupId = args.scheduleGroupId

        libScheduler.setScheduleDetails(dbCicada, scheduleId, scheduleDescription, serverId, scheduleOrder,
            isAsync, isEnabled, adhocExecute, intervalMask, firstRunDate, lastRunDate, execCommand,
            parameters, adhocParameters, scheduleGroupId)

        table = [
        ['Setting', 'Value'],
        ['scheduleId', scheduleId],
        ['scheduleDescription', scheduleDescription],
        ['serverId', serverId],
        ['scheduleOrder', scheduleOrder],
        ['isAsync', isAsync],
        ['isEnabled', isEnabled],
        ['adhocExecute', adhocExecute],
        ['intervalMask', intervalMask],
        ['firstRunDate', firstRunDate],
        ['lastRunDate', lastRunDate],
        ['execCommand', execCommand],
        ['parameters', parameters],
        ['adhocParameters', adhocParameters],
        ['scheduleGroupId', scheduleGroupId]
        ]
        print(tabulate(table, headers="firstrow", tablefmt="psql"))

        exit(0)

if __name__ == '__main__':
    main()
