"""Spread schedules accross servers."""

import sys

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


def csv_to_list(comma_separated_string: str) -> [int]:
    """Convert list of string to a list of integers"""
    try:
        return list(map(int, comma_separated_string.split(',')))
    except ValueError:
        print('ERROR: Cannot convert list of strings to list of integers')
        sys.exit(1)


def get_schedules_load_yesterday(db_cur, server_ids: [int] = None):
    """Extract details of executable of a schedule"""
    if server_ids:
        sql_server_ids = ','.join(str(server_id) for server_id in server_ids)

    sqlquery = f"""
    select
        schedule_id,
        sum(end_time - start_time) as total_run_duration
    from schedule_log
    where start_time > to_char(now() - interval '1 DAY', 'YYYY-MM-DD 00:00:00')::timestamp
        and start_time < to_char(now(), 'YYYY-MM-DD 00:00:00')::timestamp
        and server_id in ({sql_server_ids})
    group by schedule_id
    order by 2 desc
    """

    db_cur.execute(sqlquery)
    cur_schedules_load_yesterday = db_cur

    obj_schedules_load_yesterday = []
    for row in cur_schedules_load_yesterday.fetchall():
        obj_schedules_load_yesterday.append(str(row[0]))

    return obj_schedules_load_yesterday


def get_servers(db_cur, enabled_only: bool = True, server_ids: [int] = None):
    """Get active servers"""
    sql_enabled_filter = " and is_enabled = 1" if enabled_only else ""
    sql_server_id_filter = ""
    if server_ids:
        sql_server_ids = ','.join(str(server_id) for server_id in server_ids)
        sql_server_id_filter = f" and server_id in ({sql_server_ids})"

    sqlquery = f"""
    select server_id from servers
    where 1 = 1
    {sql_enabled_filter}
    {sql_server_id_filter}
    order by server_id
    """

    db_cur.execute(sqlquery)
    cur_enabled_servers = db_cur

    obj_enabled_servers = []
    for row in cur_enabled_servers.fetchall():
        obj_enabled_servers.append(str(row[0]))

    return obj_enabled_servers


@utils.named_exception_handler('spread_schedules')
def main(args, dbname = None):
    """Spread schedules accross servers."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    from_server_ids = csv_to_list(args.from_server_ids) if args.from_server_ids else None
    to_server_ids = csv_to_list(args.to_server_ids) if args.to_server_ids else None

    obj_enabled_servers = get_servers(db_cur, server_ids=to_server_ids)
    enabled_server_count = len(obj_enabled_servers)

    if enabled_server_count == 0:
        print("ERROR: No enabled target server(s)")
        sys.exit(1)

    next_enabled_server = 0

    obj_schedules_load_yesterday = get_schedules_load_yesterday(db_cur, from_server_ids)

    for schedule_id in obj_schedules_load_yesterday:

        current_schedule_details = scheduler.get_schedule_details(db_cur, schedule_id)
        new_schedule_details = current_schedule_details.copy()
        new_schedule_details['server_id'] = obj_enabled_servers[next_enabled_server]

        next_enabled_server += 1
        if next_enabled_server == enabled_server_count:
            next_enabled_server = 0

        if args.commit:
            scheduler.update_schedule_details(db_cur, new_schedule_details)
            print(f"'{str(current_schedule_details['schedule_id'])}' has been reassigned : " \
                f"{str(current_schedule_details['server_id'])} -> {str(new_schedule_details['server_id'])}")
        else:
            print(f"'{str(current_schedule_details['schedule_id'])}' will be reassigned : " \
                f"{str(current_schedule_details['server_id'])} -> {str(new_schedule_details['server_id'])}")

    db_cur.close()
    db_conn.close()
