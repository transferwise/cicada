"""List all schedule ID's."""

from tabulate import tabulate

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


@utils.named_exception_handler("list_schedule_ids")
def main(dbname=None):
    """Show all Cicada schedule ID's."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    obj_schedules = scheduler.get_all_schedule_ids(db_cur)
    db_cur.close()
    db_conn.close()
    print("")
    print(tabulate(obj_schedules, headers=["Server ID", "Schedule ID", "Description"]))
