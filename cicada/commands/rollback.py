
from typing import Optional
from cicada.lib import postgres, utils
from cicada.lib import scheduler


@utils.named_exception_handler("rollback")
def main(server_id: Optional[int] = None, schedule_id: Optional[str] = None, dbname=None, full=False):
    """
    Roll back schedules in case of issues during assignment. 
    If neither server_id and schedule_id are provided, rollback applies to all servers.

    Args:
        server_id: Optional[int] [Mutually exclusive with schedule_id]
            Target server to roll back. 
        schedule_id: Optional[int] [Mutually exclusive with server_id]
            Target schedule to roll back. 
        db_cur: Database cursor to use for the rollback operations.
        dbname: Optional[str]
            Database name to connect to if db_cur is not provided. If db_cur is provided, dbname is ignored.
        prev: bool
            If True, roll back to the previous schedule in the schedule_backups table. If False, roll back to the original schedule 
    """
    if type(server_id) != int and server_id is not None: raise TypeError(f"server_id needs to be of type int. {type(server_id)}")
    if type(schedule_id) != str and schedule_id is not None: raise TypeError("schedule_id needs to be of type str")

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    
    if not server_id and not schedule_id:
        # Recursively call rollback for each server_id if no specific server_id is provided 
        server_ids = scheduler.get_all_server_ids(db_cur)
        for id in server_ids:
            main(server_id=id[0], dbname=dbname, full=full)
        return

    if full: print("\n------------Starting RollbackTo Orig Schedules-----------------") 
    else: print("\n------------Starting Rollback To Previous Schedules-----------------")

    try:
        if server_id:
            scheduler.restore_previous_schedules(db_cur=db_cur, server_id=server_id, full=full)
            schedule_ids = [row[0] for row in scheduler.get_all_schedule_ids_per_server(db_cur, server_id)]
            schedule_masks = [scheduler.get_schedule_details(db_cur, schedule_id)["interval_mask"] for schedule_id in schedule_ids]
            print("New Schedules after rollback:\n")
            for schedule_id, schedule_mask in zip(schedule_ids, schedule_masks):
                print(f"{schedule_id} : {schedule_mask}")
        else:
            scheduler.restore_previous_schedules(db_cur=db_cur, schedule_id=schedule_id, full=full)
            schedule = scheduler.get_schedule_details(db_cur, schedule_id)
            if len(schedule) == 0:
                raise Exception(f"Schedule with schedule_id {schedule_id} not found for rollback.")
            print(f"Schedule {schedule_id} rolled back successfully to {schedule['interval_mask']}.")
        print("Rollback successful") 
        
    except Exception as e:
        print(f"Error during rollback for server_id {server_id} and schedule_id {schedule_id}: {e}")
        
    db_cur.close()
    db_conn.close()