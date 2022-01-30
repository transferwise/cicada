"""List all scheduled schedules for this server."""

import datetime

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


@utils.named_exception_handler("list_server_schedules")
def main(dbname=None):
    """Show all Cicada schedules for calling server_id."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    server_id = scheduler.get_server_id(db_cur)

    # Get all Asynchronous Schedules
    obj_schedules = scheduler.get_all_schedules(db_cur, server_id, 1)

    print("")
    print("server_id : " + server_id)
    print("now : " + str(datetime.datetime.now()))
    print("now_minute : " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:00"))
    print(
        "-------------------------------------------------------------------------------------------"
    )
    print("Asynchronous Schedules :")
    print("schedule_id\tFull Command")

    for schedule_id in obj_schedules:
        obj_schedule_details = scheduler.get_schedule_executable(db_cur, schedule_id)
        for db_row in obj_schedule_details.fetchall():
            command = str(db_row[0])
            parameters = str(db_row[1])
            print(schedule_id + "\t\t" + command + " " + parameters)

    # Get all Synchronous Schedules
    obj_schedules = scheduler.get_all_schedules(db_cur, server_id, 0)

    print("")
    print(
        "-------------------------------------------------------------------------------------------"
    )
    print("Synchronous Schedules :")
    print("schedule_id\tFull Command")

    for schedule_id in obj_schedules:
        obj_schedule_details = scheduler.get_schedule_executable(db_cur, schedule_id)
        for db_row in obj_schedule_details.fetchall():
            command = str(db_row[0])
            parameters = str(db_row[1])
            print(schedule_id + "\t\t" + command + " " + parameters)

    print("")

    db_cur.close()
    db_conn.close()
