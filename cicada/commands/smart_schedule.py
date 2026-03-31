"""Shifts the schedules on a node to distribute the load"""

from __future__ import annotations
import datetime
import sys
from croniter import croniter
from typing import Optional
from cicada.lib import postgres, utils
from cicada.lib import scheduler
from cicada.lib.SmartScheduling import pygad
from cicada.lib.SmartScheduling.domain import Tap


def get_schedules_per_server(server_id, dbname=None):
    """Get all schedules for a given server_id."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    schedule_ids = [row[0] for row in scheduler.get_all_schedule_ids_per_server(db_cur, server_id)]
    db_cur.close()
    db_conn.close()

    if not schedule_ids:
        print(f"No schedules found for server_id {server_id}")
        return []
    return schedule_ids


def find_all_server_ids(dbname=None):
    """Find all server_ids in the system."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    server_ids = scheduler.get_all_server_ids(db_cur)
    db_cur.close()
    db_conn.close()
    return server_ids


def create_tap_objects(schedule_ids, dbname=None):
    """Create Tap objects from schedule_ids."""
    
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    taps : list[Tap] = []

    # Fetch details for each schedule and convert to Tap objects
    for schedule_id in schedule_ids:
        details = scheduler.get_schedule_details(db_cur, schedule_id)
        try:
            tap = Tap(details, db_cur=db_cur)

            # Ignore the few taps that have irregular cron expressions for now. There are few enough that this shouldn't impact the optimisation and is not worth the effort to try and support these irregular schedules in the GA
            if not tap.is_regular_schedule():
                raise ValueError(f"Skipping irregular cron expression: {tap.interval_mask}")
            else:
                tap.determine_attributes(db_cur)
                taps.append(tap)

        except Exception as e:
            print(f"Skipping schedule {schedule_id} due to error: {e}")

    db_cur.close()
    db_conn.close()
    return taps

def update_schedule_cron(tap : Tap) -> str:
    """
        Uses the start_blocks to shift the cron expression accordingly. Gene space is already limited from 0 to the frequency of the tap

        Ex. form of cron expression: "8-59/15 * * * *" (every 15 minutes starting at minute 8 of each hour)
    """
    frequency = tap.frequency_minutes
    shift = tap.shift 

    if not shift or shift == 0:
        return tap  # No shift needed
    
    if frequency >= 60:
        minute = shift % 60
        hour = shift // 60 + croniter(tap.original_interval_mask).get_next(datetime.datetime).hour  # Get the hour of the first scheduled run and add the shift in hours
        tap.interval_mask = f"{minute} {hour} * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask}")
        return tap
    else:
        tap.interval_mask = f"{shift}-59/{frequency} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask}")
        return tap
    
    

def assign_new_schedules(optimised_taps: list[pygad.Tap], dbname=None):
    """Assign new schedules based on the optimal schedule found."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    # For each tap, update the schedule in the DB with the new interval_mask based on the shift calculated by the GA optimizer
    for tap in optimised_taps:
        tap = update_schedule_cron(tap)

        schedule_details = {
            "adhoc_parameters": None,
            "adhoc_execute": None,
            "schedule_group_id": None,
            "parameters": None,
            "server_id": None,
            "last_run_date": None,
            "is_enabled": None,
            "interval_mask": tap.interval_mask,
            "schedule_description": None,
            "auto_update_time": None,
            "schedule_order": None,
            "schedule_id": tap.schedule_id,
            "is_async": None,
            "abort_running": None,
            "exec_command": None,
            "first_run_date": None,
            "is_running": None
        }  
        scheduler.update_schedule_details(db_cur=db_cur, schedule_details=schedule_details)
    db_conn.commit()
    db_cur.close()
    db_conn.close()

def rollback(server_id : Optional[int], db_cur, dbname=None):
    """
    Rollback to original schedules in case of any issues during assignment.
    Args:        server_id: Optional[int] : the server_id to rollback, if None rollback all servers
    """
    
    if not server_id:
        # Recursively call rollback for each server_id if no specific server_id is provided 
        server_ids = find_all_server_ids(dbname)
        for id in server_ids:
            rollback(server_id=id[0], db_cur=db_cur, dbname=dbname)
        return

    taps = get_schedules_per_server(server_id=server_id, dbname=dbname)

    for tap in taps:
        schedule_details = {
            "adhoc_parameters": None,
            "adhoc_execute": None,
            "schedule_group_id": None,
            "parameters": None,
            "server_id": None,
            "last_run_date": None,
            "is_enabled": None,
            "interval_mask": tap.original_interval_mask,
            "schedule_description": None,
            "auto_update_time": None,
            "schedule_order": None,
            "schedule_id": tap.schedule_id,
            "is_async": None,
            "abort_running": None,
            "exec_command": None,
            "first_run_date": None,
            "is_running": None
        }  
        scheduler.update_schedule_details(db_cur=db_cur, schedule_details=schedule_details)

    raise NotImplementedError("Rollback functionality not yet implemented")


@utils.named_exception_handler("smart_schedule")
def main(server_id=None, dbname=None):

    if not server_id:
        # Recursively call main for each server_id if no specific server_id is provided 
        server_ids = find_all_server_ids(dbname)
        for id in server_ids:
            main(server_id=id[0], dbname=dbname)
        return
    
    # Get schedules for the server_id
    schedule_ids = get_schedules_per_server(server_id=server_id, dbname=dbname)
    print(f"Found schedules for server_id {server_id}: {schedule_ids}")

    # Build Tap objects
    taps = create_tap_objects(schedule_ids, dbname=dbname)
    if not taps:
        print("No valid schedules found to optimize.")
        sys.exit(1)

    # Run GA optimizer ---> add in way to change GAConfig parameters    !
    try:
        ga = pygad.GAPyGADScheduler()
        optimised_taps, start_blocks, peak_cpu, usage = ga.solve(taps)
        print(f"Optimized schedule for server_id {server_id}: {[tap.schedule_id for tap in optimised_taps]} with start blocks {start_blocks}, peak CPU {peak_cpu}, and usage {usage}")

        assign_new_schedules(optimised_taps, dbname=dbname)

    except Exception as e:
        print(f"Error during optimization for server_id {server_id}: {e}")
        sys.exit(1)

