
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
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    
    if not server_id and not schedule_id:
        # Recursively call rollback for each server_id if no specific server_id is provided 
        server_ids = scheduler.get_all_server_ids(db_cur)
        for id in server_ids:
            main(server_id=id[0], dbname=dbname, full=full)
        return

    if server_id:
        scheduler.restore_previous_schedules(db_cur=db_cur, server_id=server_id, full=full)
    else:
        scheduler.restore_previous_schedules(db_cur=db_cur, schedule_id=schedule_id, full=full)

        
    db_cur.close()
    db_conn.close()