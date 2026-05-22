from typing import Optional
from cicada.lib import postgres, utils
from cicada.lib import scheduler



def _rollback_to_previous_snapshot(db_cur, server_id):
    """
    Roll back to the previous snapshot for a given server_id. If no previous snapshot exists, perform a full rollback.
    """
    print(f"\n[Rolling back server {server_id}]")
    snapshots = scheduler.retrieve_snapshots(db_cur, server_id)
    current_snapshot = snapshots[0][0] if snapshots and len(snapshots) > 0 else None
    previous_snapshot = snapshots[1][0] if snapshots and len(snapshots) > 1 else None

    # Remove the current snapshot (if it exists) to prevent it from being restored in future rollbacks
    if current_snapshot is not None:
        scheduler.reset_schedule_backups(db_cur, snapshot_id=current_snapshot)
        scheduler.remove_snapshot(db_cur, current_snapshot)
        
    # Restore the previous snapshot if it exists. If no previous snapshot exists, perform a full rollback instead
    if previous_snapshot is not None:
        scheduler.restore_previous_schedules(db_cur, server_id=server_id, snapshot_id=previous_snapshot)
    else:
        print("No previous snapshot found. Commencing full rollback instead...\n")
        scheduler.full_rollback(db_cur, server_id)


@utils.named_exception_handler("smart_schedule_rollback")
def main(server_id: Optional[int] = None, schedule_id: Optional[str] = None, dbname=None, full=False, previous=False):
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
            sever_ids = [server_id] if server_id else scheduler.get_all_server_ids(db_cur)
            for server_id in sever_ids:
                schedule_ids = scheduler.get_all_schedule_ids_per_server(db_cur, server_id)
                scheduler.snapshot_schedules(db_cur, schedule_ids=schedule_ids, server_id=server_id, reason='Full Rollback')

        elif previous:
            print("\n------------Starting Rollback to Previous Snapshot-----------------")
            if not server_id:
                print(f"Rolling back all servers...")
                for server in scheduler.get_all_server_ids(db_cur):
                    server_id = server[0]
                    _rollback_to_previous_snapshot(db_cur, server_id)
            else:
                _rollback_to_previous_snapshot(db_cur, server_id)

    except Exception as e:
        print(f"Error during rollback: {e}")
        raise

    finally:
        db_cur.close()
        db_conn.close()
