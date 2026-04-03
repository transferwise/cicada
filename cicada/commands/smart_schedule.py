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
        sys.exit(1)

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
            if tap.is_unsupported():
                if tap.is_blacklisted():
                    print(f"Skipping blacklisted schedule {tap.schedule_id} with cron expression {tap.interval_mask}")
                elif not tap.is_regular_schedule():
                    print(f"Skipping irregular schedule {tap.schedule_id} with cron expression {tap.interval_mask}")
                else:
                    print(f"Skipping schedule {tap.schedule_id} with frequency {tap.frequency_minutes} minutes as shifting for these taps is unsupported currently")

            else:
                tap._determine_start_time_mins()
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

    if not shift:
        return tap  # No shift needed
    
    if frequency == 1440:  # For daily taps, we can shift within the hour
        hour = shift // 60 
        minute = (shift - hour * 60) % 60
        tap.interval_mask = f"{minute} {hour} * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask} for tap {tap.schedule_id}")
        return tap
    elif frequency == 60:  # For hourly taps, we can shift within the hour
        minute = shift % 60
        tap.interval_mask = f"{minute} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask} for tap {tap.schedule_id}")
        return tap
    elif frequency < 60:
        assert shift < frequency, f"Shift {shift} cannot be greater than or equal to frequency {frequency} for tap {tap.schedule_id}"
        tap.interval_mask = f"{shift}-59/{frequency} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask} for tap {tap.schedule_id}")
        return tap
        


def assign_new_schedules(optimised_taps: list[pygad.Tap], db_cur):
    """Assign new schedules based on the optimal schedule found."""

    # For each tap, update the schedule in the DB with the new interval_mask based on the shift calculated by the GA optimizer
    for tap in optimised_taps:
        previous_schedule_mask = tap.interval_mask
        tap = update_schedule_cron(tap)
        print(f"Updating schedule {tap.schedule_id} with new interval mask: {tap.interval_mask} and shift of {tap.shift} minutes")
        tap._determine_start_time_mins() 

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
            "previous_interval_mask": previous_schedule_mask,
            "interval_mask": tap.interval_mask,
            "start_time_shift_mins": tap.start_time_mins
        }
        scheduler.update_schedule_backups(db_cur, previous_schedule_details)


@utils.named_exception_handler("smart_schedule")
def main(server_id=None, dbname=None, ga_config=None):
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    if not server_id:
        # Recursively call main for each server_id if no specific server_id is provided 
        server_ids = scheduler.get_all_server_ids(db_cur)
        for id in server_ids:
            main(server_id=id[0], dbname=dbname)
        
    else:
        # Get schedules for the server_id
        schedule_ids = get_schedules_per_server(server_id=server_id, db_cur=db_cur)
        print(f"Found {len(schedule_ids)} schedules for server_id {server_id}")

        # Build Tap objects
        taps = create_tap_objects(schedule_ids, db_cur=db_cur)
        if not taps:
            print("No valid schedules found to optimize.")
            sys.exit(1)

        try:
            ga = pygad.GAPyGADScheduler(config=ga_config)
            optimised_taps, start_blocks, peak_cpu, usage, initial_fitness = ga.solve(taps)
            print(f"Optimized schedule for server_id {server_id}: new peak CPU {peak_cpu}")

            if peak_cpu < initial_fitness:  # Only update schedules if we have found an improvement
                assign_new_schedules(optimised_taps, db_cur=db_cur)
            else:
                print(f"No improvement found for server_id {server_id}. Current peak CPU: {initial_fitness}, Optimized peak CPU: {peak_cpu}. No schedule updates will be made.")

        except Exception as e:
            print(f"Error during optimization for server_id {server_id}: {e}")
            sys.exit(1)

    db_cur.close()
    db_conn.close()