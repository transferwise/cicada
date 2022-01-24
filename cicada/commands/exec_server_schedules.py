"""Execute all scheduled schedules for this server."""

import subprocess

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


@utils.named_exception_handler("exec_server_schedules")
def main(dbname=None):
    """Execute all scheduled schedules for this server."""

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    server_id = scheduler.get_server_id(db_cur)

    # Get all asynchronous jobs and execute
    obj_schedules = scheduler.get_all_schedules(db_cur, server_id, 1)

    for schedule_id in obj_schedules:
        full_command = scheduler.generate_exec_schedule_command(
            str(schedule_id), dbname
        )

        # Note : subprocess.Popen = asynchronous
        # pylint: disable=consider-using-with
        subprocess.Popen(
            full_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    # Get all synchronous jobs and execute
    obj_schedules = scheduler.get_all_schedules(db_cur, server_id, 0)

    db_cur.close()
    db_conn.close()

    for schedule_id in obj_schedules:
        full_command = scheduler.generate_exec_schedule_command(
            str(schedule_id), dbname
        )

        # Note : subprocess.call = synchronous
        subprocess.call(
            full_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
