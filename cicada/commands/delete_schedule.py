"""Delete a schedule using schedule_id."""

import sys

from tabulate import tabulate

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


@utils.named_exception_handler("delete_schedule")
def main(schedule_id, dbname=None):
    """Delete a schedule using schedule_id."""

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    scheduler.delete_schedule(db_cur, str(schedule_id))
    db_cur.close()
    db_conn.close()

    print("schedule_id '" + str(schedule_id) + "' is deleted!")
