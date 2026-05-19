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
import shlex
from croniter import croniter

from cicada.lib import postgres

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
        ,smart_interval_mask
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
        schedule_details["smart_interval_mask"] = row[10]
        schedule_details["first_run_date"] = row[11]
        schedule_details["last_run_date"] = row[12]
        schedule_details["exec_command"] = row[13]
        schedule_details["parameters"] = row[14]
        schedule_details["adhoc_parameters"] = row[15]
        schedule_details["schedule_group_id"] = row[16]

    return schedule_details


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
        sqlquery = sqlquery + " ,'" + str(schedule_details["schedule_description"]) + "'"
    if schedule_details["server_id"] is None:
        sqlquery = sqlquery + " ,(SELECT MAX(server_id) FROM servers WHERE is_enabled=1)"
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
        sqlquery = sqlquery + " ,'" + postgres.escape_upsert_string(str(schedule_details["exec_command"])) + "'"
    if schedule_details["parameters"] is not None:
        sqlquery = sqlquery + " ,'" + postgres.escape_upsert_string(str(schedule_details["parameters"])) + "'"
    if schedule_details["adhoc_parameters"] is not None:
        sqlquery = sqlquery + " ,'" + postgres.escape_upsert_string(str(schedule_details["adhoc_parameters"])) + "'"
    if schedule_details["schedule_group_id"] is not None:
        sqlquery = sqlquery + " ," + str(schedule_details["schedule_group_id"])

    sqlquery = sqlquery + ")"

    db_cur.execute(sqlquery)


def update_schedule_details(db_cur, schedule_details):
    """Update and existing schedule"""
    sqlquery = "UPDATE schedules SET"
    sqlquery = sqlquery + " schedule_id = '" + str(schedule_details["schedule_id"]) + "'"

    if schedule_details["schedule_description"] is not None:
        sqlquery = sqlquery + " ,schedule_description = '" + str(schedule_details["schedule_description"]) + "'"
    if schedule_details["server_id"] is not None:
        sqlquery = sqlquery + " ,server_id = " + str(schedule_details["server_id"])
    if schedule_details["schedule_order"] is not None:
        sqlquery = sqlquery + " ,schedule_order = " + str(schedule_details["schedule_order"])
    if schedule_details["is_async"] is not None:
        sqlquery = sqlquery + " ,is_async = " + str(schedule_details["is_async"])
    if schedule_details["is_enabled"] is not None:
        sqlquery = sqlquery + " ,is_enabled = " + str(schedule_details["is_enabled"])
    if schedule_details["adhoc_execute"] is not None:
        sqlquery = sqlquery + " ,adhoc_execute = " + str(schedule_details["adhoc_execute"])
    if schedule_details["abort_running"] is not None:
        sqlquery = sqlquery + " ,abort_running = " + str(schedule_details["abort_running"])
    if schedule_details["interval_mask"] is not None:
        sqlquery = sqlquery + " ,interval_mask = '" + str(schedule_details["interval_mask"]) + "'"
    if schedule_details["first_run_date"] is not None:
        sqlquery = sqlquery + " ,first_run_date = '" + str(schedule_details["first_run_date"]) + "'"
    if schedule_details["last_run_date"] is not None:
        sqlquery = sqlquery + " ,last_run_date = '" + str(schedule_details["last_run_date"]) + "'"
    if schedule_details["exec_command"] is not None:
        sqlquery = (
            sqlquery + " ,exec_command = '" + postgres.escape_upsert_string(str(schedule_details["exec_command"])) + "'"
        )
    if schedule_details["parameters"] is not None:
        sqlquery = (
            sqlquery + " ,parameters = '" + postgres.escape_upsert_string(str(schedule_details["parameters"])) + "'"
        )
    if schedule_details["adhoc_parameters"] is not None:
        sqlquery = (
            sqlquery
            + " ,adhoc_parameters = '"
            + postgres.escape_upsert_string(str(schedule_details["adhoc_parameters"]))
            + "'"
        )
    if schedule_details["schedule_group_id"] is not None:
        sqlquery = sqlquery + " ,schedule_groupId = " + str(schedule_details["schedule_group_id"])
    sqlquery = sqlquery + " WHERE schedule_id = '" + str(schedule_details["schedule_id"]) + "'"

    db_cur.execute(sqlquery)


def update_schedule_details_bulk(db_cur, schedule_list):
    """Update multiple schedules in a single bulk query using CASE statements.

    Args:
        db_cur: Database cursor
        schedule_list: List of dicts, each with schedule_id and any fields to update

    """
    if not schedule_list:
        return

    schedule_ids = [str(s["schedule_id"]) for s in schedule_list]
    columns_to_update = set()

    for schedule in schedule_list:
        columns_to_update.update(k for k, v in schedule.items() if k != "schedule_id" and v is not None)

    if not columns_to_update:
        print("No fields to update for any schedules. Bulk update skipped.")
        return

    case_clauses = []

    for col in sorted(columns_to_update):
        case_whens = []
        for schedule in schedule_list:
            if col in schedule and schedule[col] is not None:
                val = schedule[col]
                if col in ["schedule_description", "interval_mask", "smart_interval_mask", "first_run_date",
                           "last_run_date", "exec_command", "parameters", "adhoc_parameters"]:
                    if col in ["exec_command", "parameters", "adhoc_parameters"]:
                        escaped_val = postgres.escape_upsert_string(str(val))
                    else:
                        escaped_val = str(val)
                    case_whens.append(f"WHEN schedule_id = '{str(schedule['schedule_id'])}' THEN '{escaped_val}'")
                else:
                    case_whens.append(f"WHEN schedule_id = '{str(schedule['schedule_id'])}' THEN {val}")

        if case_whens:
            case_clauses.append(f"{col} = CASE {' '.join(case_whens)} ELSE {col} END")

    if not case_clauses:
        print("No fields to update for any schedules. Bulk update skipped.")
        return

    sqlquery = f"UPDATE schedules SET {', '.join(case_clauses)} WHERE schedule_id = ANY(%s)"
    db_cur.execute(sqlquery, (schedule_ids,))
    return 


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


def get_full_command(command, parameters):
    """Generate Full Command"""
    full_command = []
    full_command.extend(shlex.split(command))
    full_command.extend(shlex.split(parameters))
    return full_command


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
            COALESCE(smart_interval_mask, interval_mask),
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

    now_minute = datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00"), "%Y-%m-%d %H:%M:%S")

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
            iteration = croniter(interval_mask, now_minute - datetime.timedelta(minutes=1))
            next_iter = iteration.get_next(datetime.datetime)

            if now_minute == next_iter:
                obj_schedules.append(schedule_id)

    return obj_schedules


def get_all_schedule_ids(db_cur):
    sqlquery = "SELECT server_id, schedule_id, schedule_description from schedules"
    db_cur.execute(sqlquery)
    cur_schedules = db_cur

    schedule_ids = cur_schedules.fetchall()
    return schedule_ids


def delete_schedule(db_cur, schedule_id):
    sqlquery = f"DELETE from schedules WHERE schedule_id = '{schedule_id}'"
    db_cur.execute(sqlquery)



def get_all_server_ids(db_cur):
    """Get all possible server_ids from the servers table"""
    sqlquery = "SELECT DISTINCT server_id FROM schedules ORDER BY server_id"
    db_cur.execute(sqlquery)
    server_ids = db_cur.fetchall()

    return server_ids

def get_all_schedule_ids_per_server(db_cur, server_id):
    """Get all possible schedule_ids for each server from the schedules table"""
    sqlquery = """
        SELECT DISTINCT schedule_id
        FROM schedules
        WHERE server_id = %s
        ORDER BY schedule_id
        """
    db_cur.execute(sqlquery, (server_id,))
    schedule_ids = db_cur.fetchall()

    return schedule_ids


def get_median_run_time(db_cur, schedule_id):
    """
    Calculate the median runtime in minutes for a schedule_id from the schedule_log table. 

    Zero runs => 5 mins (conservative estimate, allows local testing without data and for new schedules to be 
    scheduled without having to wait for historical data to be collected. 
    """

    sqlquery = f"""
        SELECT percentile_cont(0.5)
        WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (end_time - start_time)) / 60)
            AS median_minutes_taken
        FROM schedule_log
        WHERE schedule_id = '{schedule_id}'
    """
    db_cur.execute(sqlquery)
    row = db_cur.fetchone()

    try:
        average_runtime_minutes = float(row[0])
        return average_runtime_minutes
    except Exception:
        # No runs -> assigns default runtime of 5 minutes
        return 5


def retrieve_snapshots(db_cur, server_id):
    """
    Retrieve all snapshots for a schedule in reverse chronological order.
    """
    sqlquery = """
        SELECT DISTINCT(snapshot_id), snapshot_timestamp
        FROM schedule_backups
        WHERE server_id = %s
        ORDER BY snapshot_timestamp DESC
    """
    db_cur.execute(sqlquery, (server_id,))
    return db_cur.fetchall()


def get_schedules_by_snapshot_id(db_cur, snapshot_id, server_id):
    """
    Fetch a specific snapshot by ID.
    """
    sqlquery = """
        SELECT
            schedule_id,
            interval_mask, 
            smart_interval_mask
        FROM schedule_backups
        WHERE snapshot_id = %s AND server_id = %s
    """
    db_cur.execute(sqlquery, (snapshot_id, server_id))
    row = db_cur.fetchone()

    if not row:
        return None

    return {
        'schedule_id': row[0],
        'interval_mask': row[1],
        'smart_interval_mask': row[2],
    }

def full_rollback(db_cur, server_id=None, schedule_id=None):

    if server_id and schedule_id:
        raise ValueError("Cannot specify both server_id and schedule_id for full rollback, please specify only one to rollback all schedules for a server or an individual schedule respectively")
    if server_id:
        schedule_ids = [row[0] for row in get_all_schedule_ids_per_server(db_cur, server_id)]
    
    print(f"Rolling back schedules for server_id {server_id} to original interval_mask by setting smart_interval_mask to NULL...")
    print(f"Found {len(schedule_ids)} schedules to rollback for server_id {server_id}...")
    # Set smart_schedule_mask to NULL for all affected schedules to rollback to original interval_mask
    update_all_schedules_query = """
        UPDATE schedules SET smart_interval_mask = NULL WHERE schedule_id = ANY(%s)
        """
    db_cur.execute(update_all_schedules_query, (schedule_ids,))
    print(f"Schedules Updated: {schedule_ids}")
    return


def restore_previous_schedules(db_cur, server_id, snapshot_id=None):
    """
    Restore schedules from snapshots.
    """
    if not snapshot_id:
        snapshot_id = retrieve_snapshots(db_cur, server_id)[0][0]

    print(f"Restoring schedules for server_id {server_id} from snapshot_id {snapshot_id}")
    print("Skipping any schedules that aren't in the snapshot or have a different interval mask...")
    sqlquery = """
        UPDATE schedules
        SET smart_interval_mask = schedule_backups.smart_interval_mask
        FROM schedule_backups
        WHERE schedules.schedule_id = schedule_backups.schedule_id
        AND schedules.server_id = %s
        AND schedule_backups.snapshot_id = %s
        AND schedules.interval_mask = schedule_backups.interval_mask
    """
    db_cur.execute(sqlquery, (server_id, snapshot_id))
    return


def get_blocklisted_schedule_ids(db_cur, server_id=None):
    """Get a list of schedule_ids that are blocklisted from optimization"""
    sqlquery = "SELECT schedule_id FROM schedule_blocklist"
    if server_id:
        sqlquery += " WHERE server_id = %s"
        db_cur.execute(sqlquery, (server_id,))
    else:
        db_cur.execute(sqlquery)
    blocklist_schedule_ids = [row[0] for row in db_cur.fetchall()]
    return blocklist_schedule_ids


def reset_schedule_backups(db_cur):
    """Reset schedule_backups table by deleting all entries, used before creating new backups"""
    sqlquery = "DELETE FROM schedule_backups"
    db_cur.execute(sqlquery)
    return 