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
    parser.add_argument('--scheduleGroupId', type=int, help='Optional field to help with schedule grouping')
    args = parser.parse_args()

    # Get schedule details
    dbCicada = libPgSQL.init_db()
    currentScheduleDetails = libScheduler.getScheduleDetails(dbCicada, str(args.scheduleId))

    # Perform "show" command only if currentScheduleDetails has values
    if args.command == 'show':
        if currentScheduleDetails:
            print(tabulate(currentScheduleDetails.items(), ['Detail', 'Value'], tablefmt="psql"))
            exit(0)
        else:
            print('ERROR: scheduleId \'' + str(args.scheduleId) + '\' not found')
            exit(1)  


    # Perform "upsert" command
    newScheduleDetails = dict()
    if (args.command == 'upsert'):
        # Upsert with no currentScheduleDetails = insert Schedule
        if not currentScheduleDetails:
            if args.intervalMask is None:
                print('ERROR: intervalMask is required for new schedule')
                exit(1)

            if args.execCommand is None:
                print('ERROR: execCommand is required for a new schedule')
                exit(1)

            if args.serverId is None:
                args.serverId = 1

            if args.scheduleOrder is None:
                args.scheduleOrder = 1

            newScheduleDetails['scheduleId'] = str(args.scheduleId)
            newScheduleDetails['scheduleDescription'] = args.scheduleDescription
            newScheduleDetails['serverId'] = args.serverId
            newScheduleDetails['scheduleOrder'] = args.scheduleOrder
            newScheduleDetails['isAsync'] = args.isAsync
            newScheduleDetails['isEnabled'] = args.isEnabled
            newScheduleDetails['adhocExecute'] = args.adhocExecute
            newScheduleDetails['intervalMask'] = args.intervalMask
            newScheduleDetails['firstRunDate'] = args.firstRunDate
            newScheduleDetails['lastRunDate'] = args.lastRunDate
            newScheduleDetails['execCommand'] = args.execCommand
            newScheduleDetails['parameters'] = args.parameters
            newScheduleDetails['adhocParameters'] = args.adhocParameters
            newScheduleDetails['scheduleGroupId'] = args.scheduleGroupId

            libScheduler.insertScheduleDetails(dbCicada, newScheduleDetails)

        # Upsert with currentScheduleDetails = update Schedule
        else:
            # write all current values into new values
            # overwrite all new values where args is set
            libScheduler.updateScheduleDetails(dbCicada, currentScheduleDetails)
            
            # libScheduler.updateScheduleDetails(dbCicada, newScheduleDetails)

    print(tabulate(newScheduleDetails.items(), ['Detail', 'Value'], tablefmt="psql"))
    exit(0)


if __name__ == '__main__':
    main()
