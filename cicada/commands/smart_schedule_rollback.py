from typing import Optional
from cicada.lib import postgres, utils
from cicada.lib import scheduler


@utils.named_exception_handler("smart_schedule_rollback")
def main(server_id: Optional[int] = None, schedule_id: Optional[str] = None, dbname=None, full=False, previous=False, snapshot_id: Optional[int] = None):
    """
    Roll back schedules after smart_schedule optimization.

    Args:
        server_id: Optional[int] [Mutually exclusive with schedule_id]
            Target server to roll back.
        schedule_id: Optional[str] [Mutually exclusive with server_id]
            Target schedule to roll back.
        dbname: Optional[str]
            Database name to connect to.
        full: bool
            If True, set smart_interval_mask to NULL (revert to original interval_mask).
        previous: bool
            If True, restore to the most recent snapshot (step back one optimization).
        snapshot_id: Optional[int]
            Specific snapshot_id to restore to (used with --previous).
    """
    if type(server_id) != int and server_id is not None:
        raise TypeError(f"server_id needs to be of type int. {type(server_id)}")
    if type(schedule_id) != str and schedule_id is not None:
        raise TypeError("schedule_id needs to be of type str")

    if not full and not previous:
        raise ValueError("Either --full or --previous flag must be provided")

    if full and previous:
        raise ValueError("Cannot use both --full and --previous flags")

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    try:
        if full:
            print("\n------------Starting Full Rollback-----------------")
            scheduler.full_rollback(db_cur, server_id, schedule_id)
            print("Full rollback successful\n")

        elif previous:
            print("\n------------Starting Rollback to Previous Snapshot-----------------")
            if not server_id:
                raise ValueError("server_id must be provided for rollback to previous snapshot, rollback for individual schedules must be a full rollback")
            scheduler.restore_previous_schedules(db_cur, server_id=server_id, snapshot_id=snapshot_id)
            print("Rollback to previous snapshot successful\n")

    except Exception as e:
        print(f"Error during rollback: {e}")
        raise

    finally:
        db_cur.close()
        db_conn.close()
