"""Add or remove schedules from the blocklist (excluded from smart scheduling optimization)."""

from typing import Optional
from cicada.lib import postgres, utils
from cicada.lib import scheduler


@utils.named_exception_handler("blocklist_schedule")
def main(schedule_id: str, remove: bool = False, reason: Optional[str] = None, dbname=None):
    """
    Add or remove a schedule from the blocklist.

    Blocklisted schedules are excluded from smart scheduling optimizations.

    Args:
        schedule_id: The schedule_id to blocklist or unblocklist.
        remove: True to remove from blocklist, False to add to blocklist.
        reason: Optional reason for blocklisting (used when remove=False).
        dbname: Optional database name to connect to.
    """

    if not schedule_id or not isinstance(schedule_id, str):
        raise TypeError("schedule_id must be a non-empty string")

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    try:
        if remove:
            scheduler.remove_blocklist_schedule(db_cur, schedule_id=schedule_id)
            print(f"Schedule {schedule_id} has been removed from the blocklist successfully.")
            scheduler.full_rollback(db_cur, schedule_id=schedule_id)
            print(f"Schedule {schedule_id} has been rolled back to original settings successfully.")
            scheduler.reset_schedule_backups(db_cur, schedule_id=schedule_id)
            print(f"Backups for schedule {schedule_id} have been removed successfully.")
        
        else:
            schedule_details = scheduler.get_schedule_details(db_cur, schedule_id)
            if not schedule_details or not schedule_details.get('schedule_id'):
                print(f"ERROR: Schedule {schedule_id} not found")
                return
            scheduler.blocklist_schedule(db_cur, schedule_id=schedule_id, reason=reason)
            print(f"Schedule {schedule_id} has been blocklisted successfully.")



    except Exception as e:
        print(f"Error during blocklist operation: {e}")
        raise

    finally:
        db_cur.close()
        db_conn.close()
