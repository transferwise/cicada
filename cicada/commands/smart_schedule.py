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


def get_schedules_per_server(server_id, db_cur=None):
    """Get all schedules for a given server_id."""
    schedule_ids = [row[0] for row in scheduler.get_all_schedule_ids_per_server(db_cur, server_id)]

    if not schedule_ids:
        print(f"No schedules found for server_id {server_id}")

    return schedule_ids



def create_tap_objects(schedule_ids, db_cur):
    """Create Tap objects from schedule_ids."""
    
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
        hour = shift // 60 + croniter(tap.interval_mask).get_next(datetime.datetime).hour  # Get the hour of the first scheduled run and add the shift in hours
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


def assign_new_schedules(optimised_taps: list[pygad.Tap], db_cur):
    """Assign new schedules based on the optimal schedule found."""

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

        previous_schedule_details = {
            "schedule_id": tap.schedule_id,
            "server_id": tap.server_id,
            "previous_interval_mask": tap.original_interval_mask,
            "interval_mask": tap.interval_mask,
            "start_time_shift_mins": tap.shift or 0
        }
        scheduler.update_schedule_backups(db_cur, previous_schedule_details)


@utils.named_exception_handler("smart_schedule")
def main(server_id=None, dbname=None):
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    if not server_id:
        # Recursively call main for each server_id if no specific server_id is provided 
        server_ids = scheduler.get_all_server_ids(db_cur)
        for id in server_ids:
            main(server_id=id[0], dbname=dbname)
        return
    
    # Get schedules for the server_id
    schedule_ids = get_schedules_per_server(server_id=server_id, db_cur=db_cur)
    print(f"Found schedules for server_id {server_id}: {schedule_ids}")

    # Build Tap objects
    taps = create_tap_objects(schedule_ids, db_cur=db_cur)
    if not taps:
        print("No valid schedules found to optimize.")
        sys.exit(1)

    # Run GA optimizer ---> add in way to change GAConfig parameters    !
    try:
        ga = pygad.GAPyGADScheduler()
        optimised_taps, start_blocks, peak_cpu, usage = ga.solve(taps)
        print(f"Optimized schedule for server_id {server_id}: new peak CPU {peak_cpu}")

        assign_new_schedules(optimised_taps, db_cur=db_cur)

    except Exception as e:
        print(f"Error during optimization for server_id {server_id}: {e}")
        sys.exit(1)

    db_cur.close()
    db_conn.close()