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

    db_cur.execute("BEGIN;")
    try:
        # Remove the current snapshot (if it exists) to prevent it from being restored in future rollbacks
        if current_snapshot is not None:
            scheduler.reset_schedule_backups(db_cur, snapshot_id=current_snapshot)
            scheduler.remove_snapshot(db_cur, current_snapshot)
            
        # Restore the previous snapshot if it exists. If no previous snapshot exists, perform a full rollback instead
        if previous_snapshot is not None:
            scheduler.restore_previous_schedules(db_cur, server_id=server_id, snapshot_id=previous_snapshot)
        else:
            print("No previous snapshot found. Commencing full rollback instead...\n")
            scheduler.full_rollback(db_cur, server_id=server_id)
        db_cur.execute("COMMIT;")
    except Exception as e:
        db_cur.execute("ROLLBACK;")
        print("Database changes have been rolled back due to the error.")
        raise Exception(f"Error during rollback to previous snapshot for server_id {server_id}: {e}")


@utils.named_exception_handler("smart_schedule_rollback")
def main(server_id: Optional[int] = None, schedule_id: Optional[str] = None, dbname=None, full=False, previous=False):
    """
    Roll back schedules after smart_schedule optimization.

    Args:
        server_id: Optional[int] [Mutually exclusive with schedule_id]
            Target server to roll back.
        schedule_id: Optional[str] [Mutually exclusive with server_id]
            Target schedule to roll back - can only be used with --full flag.
        dbname: Optional[str]
            Database name to connect to.
        full: bool
            If used, sets smart_interval_mask to NULL (revert to original interval_mask).
        previous: bool
            If used, restores to the most recent snapshot (step back one optimization).
    """
    if type(server_id) != int and server_id is not None:
        raise TypeError(f"server_id needs to be of type int. {type(server_id)}")
    if type(schedule_id) != str and schedule_id is not None:
        raise TypeError("schedule_id needs to be of type str")
    if not(full or previous) or (full and previous):
        raise ValueError("Exactly one of --full or --previous flags must be provided")
    if schedule_id and not full:
        raise ValueError("schedule_id can only be used with --full flag")

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    try:
        if full:
            print("\n------------Starting Full Rollback-----------------")
            db_cur.execute("BEGIN;")
            try:
                scheduler.full_rollback(db_cur, server_id, schedule_id)
                print("Full rollback successful\n")
                server_ids = [server_id] if server_id else [server[0] for server in scheduler.get_all_server_ids(db_cur)]
                for server in server_ids:
                    schedule_ids = [row[0] for row in scheduler.get_all_schedule_ids_per_server(db_cur, server)]
                    scheduler.snapshot_schedules(db_cur, schedule_ids=schedule_ids, server_id=server, reason='Full Rollback')
                db_cur.execute("COMMIT;")
            except Exception as e:
                db_cur.execute("ROLLBACK;")
                raise Exception(f"Error during full rollback: {e}")

        elif previous:
            print("\n------------Starting Rollback to Previous Snapshot-----------------")
            if not server_id:
                print(f"Rolling back all servers...")
                for server_id in scheduler.get_all_server_ids(db_cur):
                    _rollback_to_previous_snapshot(db_cur, server_id=server_id[0])
            else:
                _rollback_to_previous_snapshot(db_cur, server_id=server_id)

    except Exception as e:
        print(f"Error during rollback: {e}")
        raise

    finally:
        db_cur.close()
        db_conn.close()
