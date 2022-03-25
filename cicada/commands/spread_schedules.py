"""Spread schedules accross servers."""

import datetime
import sys

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


def csv_to_list(comma_separated_string: str) -> [int]:
    """Convert list of string to a list of integers"""
    try:
        return list(map(int, comma_separated_string.split(",")))
    except ValueError:
        print("ERROR: Cannot convert list of strings to list of integers")
        sys.exit(1)


def get_last_week_schedules_by_load(db_cur, server_ids: [int] = None):
    """Extract details of executable of a schedule"""
    if server_ids:
        sql_server_ids = ",".join(str(server_id) for server_id in server_ids)

    now = datetime.datetime.now()

    sqlquery = f"""
    SELECT
        sl.schedule_id as schedule_id,
        sum(sl.end_time - sl.start_time) as total_run_duration
    FROM schedule_log sl
        INNER JOIN schedules s USING (schedule_id)
    WHERE sl.start_time > to_char('{now}'::timestamp - interval '7 DAY', 'YYYY-MM-DD 00:00:00')::timestamp
        AND sl.start_time < to_char('{now}'::timestamp, 'YYYY-MM-DD 00:00:00')::timestamp
        AND s.server_id in ({sql_server_ids})
    GROUP BY sl.schedule_id
    ORDER BY total_run_duration DESC, sl.schedule_id ASC
    """

    db_cur.execute(sqlquery)
    cur_schedules_load_yesterday = db_cur

    last_week_schedules_by_load = []
    for row in cur_schedules_load_yesterday.fetchall():
        last_week_schedules_by_load.append(str(row[0]))

    return last_week_schedules_by_load


def get_enabled_servers(db_cur, enabled_only: bool = True, server_ids: [int] = None):
    """Get valid servers"""
    sql_enabled_filter = " and is_enabled = 1" if enabled_only else ""
    sql_server_id_filter = ""
    if server_ids:
        sql_server_ids = ",".join(str(server_id) for server_id in server_ids)
        sql_server_id_filter = f" and server_id in ({sql_server_ids})"

    sqlquery = f"""
    SELECT server_id FROM servers
    WHERE 1 = 1
    {sql_enabled_filter}
    {sql_server_id_filter}
    ORDER BY server_id
    """

    db_cur.execute(sqlquery)

    enabled_servers = []
    for row in db_cur.fetchall():
        enabled_servers.append(str(row[0]))

    return enabled_servers


@utils.named_exception_handler("spread_schedules")
def main(spread_details, dbname=None):
    """Spread schedules accross servers."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    from_server_ids = csv_to_list(spread_details["from_server_ids"])
    to_server_ids = csv_to_list(spread_details["to_server_ids"])

    valid_target_servers = get_enabled_servers(db_cur, server_ids=to_server_ids)
    valid_server_count = len(valid_target_servers)

    if valid_server_count == 0:
        print("ERROR: No enabled to_server_ids")
        sys.exit(1)

    next_enabled_server = 0

    last_week_schedules_by_load = get_last_week_schedules_by_load(
        db_cur, from_server_ids
    )

    for schedule_id in last_week_schedules_by_load:

        current_schedule_details = scheduler.get_schedule_details(db_cur, schedule_id)
        new_schedule_details = current_schedule_details.copy()
        new_schedule_details["server_id"] = valid_target_servers[next_enabled_server]

        next_enabled_server += 1
        if next_enabled_server == valid_server_count:
            next_enabled_server = 0

        if spread_details["commit"] is True:
            output_message = (
                f"'{str(current_schedule_details['schedule_id'])}' has been reassigned : "
                f"{str(current_schedule_details['server_id'])} -> {str(new_schedule_details['server_id'])}"
            )

            if (
                (spread_details["force"] is True)
                and (current_schedule_details["is_running"] == 1)
                and (
                    current_schedule_details["server_id"]
                    != new_schedule_details["server_id"]
                )
            ):
                new_schedule_details["abort_running"] = 1
                new_schedule_details["adhoc_execute"] = 1
                output_message += " | Forced abort_running and adhoc_execute"

            scheduler.update_schedule_details(db_cur, new_schedule_details)
        else:
            output_message = (
                f"'{str(current_schedule_details['schedule_id'])}' will be reassigned : "
                f"{str(current_schedule_details['server_id'])} -> {str(new_schedule_details['server_id'])}"
            )

        print(output_message)

    db_cur.close()
    db_conn.close()
