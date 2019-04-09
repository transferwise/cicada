#!/usr/bin/python
import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import libPgSQL

commands = [
  'upsert_by_id',
  'upsert_by_name'
]

def main():
    parser = argparse.ArgumentParser(description='Add or change a schedule in Cicada', add_help=True)
    parser.add_argument('command', type=str, help=', '.join(commands))
    parser.add_argument('--schedule_id', type=int, help="Id of the schedule")
    parser.add_argument('--schedule_name', type=str, help="Name of schedule")
    parser.add_argument('--server_id', type=int, default=1, help="Id of the server where the job will run")
    parser.add_argument('--schedule_order', type=int, default=1, help="run order for the schedule. lowest is first. is_async jobs will be executed all at once")
    parser.add_argument('--is_async', type=str, default=1, help="0=disabled 1=enabled | is_async jobs execute in parallel")
    parser.add_argument('--is_enabled', type=str, default=0, help="0=Disabled 1=Enabled")
    parser.add_argument('--interval_mask', type=str, help="When to execute the command | Modeled on unix crontab (minute hour dom month dow)")
    parser.add_argument('--first_run_date', type=datetime, default='1000-01-01 00:00:00.000', help="The schedule will not execute before this datetime")
    parser.add_argument('--last_run_date', type=datetime, default='9999-12-31 23:59:59.999', help="The schedule will not execute after this datetime")
    parser.add_argument('--command', type=str, help="Command to execute")
    parser.add_argument('--parameters', type=str, help="Exact string of parameters for command")
    parser.add_argument('--adhoc_parameters', type=str, help="If specified, will override parameters for next run")
    parser.add_argument('--adhoc_execute', type=str, help="0=Disabled 1=Enabled | The job will execute at next minute, regardless of other schedule time settings")
    parser.add_argument('--schedule_group_id', type=str, help="Optional field to help with schedule grouping")
    parser.add_argument('--schedule_comments', type=str, help="Schedule Comments")
    # parser.add_argument('--log', type=str, default='*', help="file to log into")


    args = parser.parse_args()

    dbCicada = libPgSQL.init_db()

if __name__ == "__main__":
    main()
