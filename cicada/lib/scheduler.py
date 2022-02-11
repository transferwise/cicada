"""Schedule management library."""

# +--------- minute (0 - 59)
# | +--------- hour (0 - 23)
# | | +--------- day of the month (1 - 31)
# | | | +--------- month (1 - 12)
# | | | | +--------- day of the week (0 - 7) (Sunday to Saturday; 7 is also Sunday)
# * * * * *

import sys
import os
import datetime
import socket
from croniter import croniter

from . import utils


def get_host_details():
    """Get details of this host"""
    hostname = socket.gethostname()
    if hostname.find(".") != -1:
        hostname = hostname[: hostname.find(".")]

    fqdn = socket.getfqdn()

    ip4_address = socket.gethostbyname(fqdn)

    host_details = {"hostname": hostname, "fqdn": fqdn, "ip4_address": ip4_address}

    return host_details


def generate_exec_schedule_command(schedule_id: str, dbname=None):
    """Generate Full Command"""
    full_command = []
    full_command.append(f"{os.path.abspath(os.path.dirname(sys.argv[0]))}/cicada")
    full_command.append("exec_schedule")
    full_command.append(f"--schedule_id={schedule_id}")

    if dbname is not None:
        full_command.append(dbname)

    return full_command


def get_server_id(db_cur):
    """Get cicada server_id of this host"""
    host_details = get_host_details()

    sqlquery = f"""
    SELECT server_id
    FROM servers
    WHERE hostname='{host_details['hostname']}'
    """

    db_cur.execute(sqlquery)
    row = db_cur.fetchone()

    try:
        server_id = str(row[0])
        return server_id
    except Exception:
        print(f"ERROR : host {host_details['hostname']} not defined in table servers")
        sys.exit(1)


def get_schedule_details(db_cur, schedule_id):
    """Extract details of a schedule"""
    sqlquery = f"""
    SELECT
        schedule_id
        ,schedule_description
        ,server_id
        ,schedule_order
        ,is_async
        ,is_enabled
        ,adhoc_execute
        ,is_running
        ,abort_running
        ,interval_mask
        ,first_run_date
        ,last_run_date
        ,exec_command
        ,parameters
        ,adhoc_parameters
        ,schedule_group_id
    FROM schedules
        WHERE schedule_id = '{str(schedule_id)}'
    LIMIT 1
    """

    db_cur.execute(sqlquery)

    schedule_details = {}

    for row in db_cur.fetchall():
        schedule_details["schedule_id"] = row[0]
        schedule_details["schedule_description"] = row[1]
        schedule_details["server_id"] = row[2]
        schedule_details["schedule_order"] = row[3]
        schedule_details["is_async"] = row[4]
        schedule_details["is_enabled"] = row[5]
        schedule_details["adhoc_execute"] = row[6]
        schedule_details["is_running"] = row[7]
        schedule_details["abort_running"] = row[8]
        schedule_details["interval_mask"] = row[9]
        schedule_details["first_run_date"] = row[10]
        schedule_details["last_run_date"] = row[11]
        schedule_details["exec_command"] = row[12]
        schedule_details["parameters"] = row[13]
        schedule_details["adhoc_parameters"] = row[14]
        schedule_details["schedule_group_id"] = row[15]

    return schedule_details


# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
def insert_schedule_details(db_cur, schedule_details):
    """Insert a new schedule"""
    sqlquery = "INSERT INTO schedules (schedule_id"
    if schedule_details["schedule_description"] is not None:
        sqlquery = sqlquery + " ,schedule_description"
    sqlquery = sqlquery + " ,server_id"
    if schedule_details["schedule_order"] is not None:
        sqlquery = sqlquery + " ,schedule_order"
    if schedule_details["is_async"] is not None:
        sqlquery = sqlquery + " ,is_async"
    if schedule_details["is_enabled"] is not None:
        sqlquery = sqlquery + " ,is_enabled"
    if schedule_details["adhoc_execute"] is not None:
        sqlquery = sqlquery + " ,adhoc_execute"
    if schedule_details["abort_running"] is not None:
        sqlquery = sqlquery + " ,abort_running"
    if schedule_details["interval_mask"] is not None:
        sqlquery = sqlquery + " ,interval_mask"
    if schedule_details["first_run_date"] is not None:
        sqlquery = sqlquery + " ,first_run_date"
    if schedule_details["last_run_date"] is not None:
        sqlquery = sqlquery + " ,last_run_date"
    if schedule_details["exec_command"] is not None:
        sqlquery = sqlquery + " ,exec_command"
    if schedule_details["parameters"] is not None:
        sqlquery = sqlquery + " ,parameters"
    if schedule_details["adhoc_parameters"] is not None:
        sqlquery = sqlquery + " ,adhoc_parameters"
    if schedule_details["schedule_group_id"] is not None:
        sqlquery = sqlquery + " ,schedule_group_id"

    sqlquery = sqlquery + ") VALUES ('" + str(schedule_details["schedule_id"]) + "'"

    if schedule_details["schedule_description"] is not None:
        sqlquery = (
            sqlquery + " ,'" + str(schedule_details["schedule_description"]) + "'"
        )
    if schedule_details["server_id"] is None:
        sqlquery = (
            sqlquery + " ,(SELECT MIN(server_id) FROM servers WHERE is_enabled=1)"
        )
    else:
        sqlquery = sqlquery + " ," + str(schedule_details["server_id"])
    if schedule_details["schedule_order"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["schedule_order"])
    if schedule_details["is_async"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["is_async"])
    if schedule_details["is_enabled"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["is_enabled"])
    if schedule_details["adhoc_execute"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["adhoc_execute"])
    if schedule_details["abort_running"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["abort_running"])
    if schedule_details["interval_mask"] is not None:
        sqlquery = sqlquery + " ,'" + str(schedule_details["interval_mask"]) + "'"
    if schedule_details["first_run_date"] is not None:
        sqlquery = sqlquery + " ,'" + str(schedule_details["first_run_date"]) + "'"
    if schedule_details["last_run_date"] is not None:
        sqlquery = sqlquery + " ,'" + str(schedule_details["last_run_date"]) + "'"
    if schedule_details["exec_command"] is not None:
        sqlquery = sqlquery + " ,'" + str(schedule_details["exec_command"]) + "'"
    if schedule_details["parameters"] is not None:
        sqlquery = sqlquery + " ,'" + str(schedule_details["parameters"]) + "'"
    if schedule_details["adhoc_parameters"] is not None:
        sqlquery = sqlquery + " ,'" + str(schedule_details["adhoc_parameters"]) + "'"
    if schedule_details["schedule_group_id"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["schedule_group_id"])

    sqlquery = sqlquery + ")"

    db_cur.execute(sqlquery)


# pylint: disable=too-many-branches
def update_schedule_details(db_cur, schedule_details):
    """Update and existing schedule"""
    sqlquery = "UPDATE schedules SET"
    sqlquery = (
        sqlquery + " schedule_id = '" + str(schedule_details["schedule_id"]) + "'"
    )

    if schedule_details["schedule_description"] is not None:
        sqlquery = (
            sqlquery
            + " ,schedule_description = '"
            + str(schedule_details["schedule_description"])
            + "'"
        )
    if schedule_details["server_id"] is not None:
        sqlquery = sqlquery + " ,server_id = " + str(schedule_details["server_id"])
    if schedule_details["schedule_order"] is not None:
        sqlquery = (
            sqlquery + " ,schedule_order = " + str(schedule_details["schedule_order"])
        )
    if schedule_details["is_async"] is not None:
        sqlquery = sqlquery + " ,is_async = " + str(schedule_details["is_async"])
    if schedule_details["is_enabled"] is not None:
        sqlquery = sqlquery + " ,is_enabled = " + str(schedule_details["is_enabled"])
    if schedule_details["adhoc_execute"] is not None:
        sqlquery = (
            sqlquery + " ,adhoc_execute = " + str(schedule_details["adhoc_execute"])
        )
    if schedule_details["abort_running"] is not None:
        sqlquery = (
            sqlquery + " ,abort_running = " + str(schedule_details["abort_running"])
        )
    if schedule_details["interval_mask"] is not None:
        sqlquery = (
            sqlquery
            + " ,interval_mask = '"
            + str(schedule_details["interval_mask"])
            + "'"
        )
    if schedule_details["first_run_date"] is not None:
        sqlquery = (
            sqlquery
            + " ,first_run_date = '"
            + str(schedule_details["first_run_date"])
            + "'"
        )
    if schedule_details["last_run_date"] is not None:
        sqlquery = (
            sqlquery
            + " ,last_run_date = '"
            + str(schedule_details["last_run_date"])
            + "'"
        )
    if schedule_details["exec_command"] is not None:
        sqlquery = (
            sqlquery
            + " ,exec_command = '"
            + str(schedule_details["exec_command"])
            + "'"
        )
    if schedule_details["parameters"] is not None:
        sqlquery = (
            sqlquery + " ,parameters = '" + str(schedule_details["parameters"]) + "'"
        )
    if schedule_details["adhoc_parameters"] is not None:
        sqlquery = (
            sqlquery
            + " ,adhoc_parameters = '"
            + str(schedule_details["adhoc_parameters"])
            + "'"
        )
    if schedule_details["schedule_group_id"] is not None:
        sqlquery = (
            sqlquery
            + " ,schedule_groupId = "
            + str(schedule_details["schedule_group_id"])
        )
    sqlquery = (
        sqlquery + " WHERE schedule_id = '" + str(schedule_details["schedule_id"]) + "'"
    )

    db_cur.execute(sqlquery)


def get_schedule_executable(db_cur, schedule_id):
    """Extract details of executable of a schedule"""
    sqlquery = (
        """
    SELECT
        exec_command,
        COALESCE(adhoc_parameters, parameters, '') AS parameters
    FROM schedules
        WHERE schedule_id = '"""
        + str(schedule_id)
        + """'
    LIMIT 1
    """
    )

    db_cur.execute(sqlquery)
    obj_schedule_executable = db_cur
    return obj_schedule_executable


def get_all_schedules(db_cur, server_id, is_async):
    """Extract all candidate schedules for a server
    +--------- minute (0 - 59)
    | +--------- hour (0 - 23)
    | | +--------- day of the month (1 - 31)
    | | | +--------- month (1 - 12)
    | | | | +--------- day of the week (0 - 7) (Sunday to Saturday; 7 is also Sunday)
    * * * * *
    """
    sqlquery = f"""
    SELECT
      schedule_id,
      interval_mask
    FROM
      ( /* bar */
      SELECT
        schedule_id,
        interval_mask,
        exec_command,
        parameters,
        adhoc_execute,
        is_async,
        is_running
      FROM
        ( /* foo */
          (SELECT
            schedule_id,
            interval_mask,
            exec_command,
            COALESCE(adhoc_parameters, parameters, '') AS parameters,
            adhoc_execute,
            is_async,
            is_running,
            schedule_order
          FROM schedules
            INNER JOIN servers USING (server_id)
          WHERE adhoc_execute = 0
            AND server_id = {str(server_id)}
            AND schedules.is_enabled = 1
            AND servers.is_enabled = 1
            AND now() >= first_run_date
            AND now() <= last_run_date
          )

          UNION

          (SELECT
            schedule_id,
            '* * * * *' AS interval_mask,
            exec_command,
            COALESCE(adhoc_parameters, parameters, '') AS parameters,
            adhoc_execute,
            is_async,
            is_running,
            schedule_order
          FROM schedules
            INNER JOIN servers USING (server_id)
          WHERE adhoc_execute = 1
            AND server_id = {str(server_id)}
            AND servers.is_enabled = 1
          )
        ) foo
      ORDER BY schedule_order, schedule_id
      ) bar

    WHERE is_running = 0
        AND is_async = {str(is_async)}
    """

    db_cur.execute(sqlquery)
    cur_schedules = db_cur

    obj_schedules = []

    now_minute = datetime.datetime.strptime(
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00"), "%Y-%m-%d %H:%M:%S"
    )

    for row in cur_schedules.fetchall():
        schedule_id = str(row[0])
        interval_mask = str(row[1])

        # Skip entries with a bad interval_mask
        if not croniter.is_valid(interval_mask):
            utils.send_slack_message(
                f":warning:  *WARNING* invalid interval_mask on schedule_id *'{schedule_id}'*",
                f"```schedule_id   : {schedule_id}\ninterval_mask : {interval_mask}```",
                "warning",
            )
        else:
            iteration = croniter(
                interval_mask, now_minute - datetime.timedelta(minutes=1)
            )
            next_iter = iteration.get_next(datetime.datetime)

            if now_minute == next_iter:
                obj_schedules.append(schedule_id)

    return obj_schedules
