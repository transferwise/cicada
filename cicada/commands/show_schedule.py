"""List a schedule using schedule_id."""

import sys

from tabulate import tabulate

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


@utils.named_exception_handler("show_schedule")
def main(schedule_id, dbname=None):
    """List a schedule using schedule_id."""

    # Get schedule details
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    current_schedule_details = scheduler.get_schedule_details(db_cur, str(schedule_id))
    db_cur.close()
    db_conn.close()

    if current_schedule_details:
        print(
            tabulate(
                current_schedule_details.items(), ["Detail", "Value"], tablefmt="psql"
            )
        )
    else:
        print("ERROR: schedule_id '" + str(schedule_id) + "' not found")
        sys.exit(1)
