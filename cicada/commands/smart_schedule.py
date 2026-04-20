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


def _get_schedules_per_server(server_id, db_cur=None):
    """Get all schedules for a given server_id."""
    existing_servers = [server[0] for server in scheduler.get_all_server_ids(db_cur)]
    if server_id not in existing_servers: raise ValueError(f"server_id not in list of existing servers. Existing servers: {existing_servers}")
    schedule_ids = [row[0] for row in scheduler.get_all_schedule_ids_per_server(db_cur, server_id)]

    if not schedule_ids:
        print(f"No schedules found for server_id {server_id}")
        sys.exit(1)

    return schedule_ids



def _create_tap_objects(schedule_ids, db_cur):
    """Create Tap objects from schedule_ids."""
    
    taps : list[Tap] = []
    blacklisted_taps = scheduler.get_blacklisted_schedule_ids(db_cur)

    # Fetch details for each schedule and convert to Tap objects
    for schedule_id in schedule_ids:
        details = scheduler.get_schedule_details(db_cur, schedule_id)
        if schedule_id in blacklisted_taps:
            details['blacklisted'] = True
        else:
            details['blacklisted'] = False

        try:
            tap = Tap(details, db_cur=db_cur)
            # Ignore the few taps that have irregular cron expressions for now. There are few enough that this shouldn't impact the optimisation and is not worth the effort to try and support these irregular schedules in the GA
            if not tap.is_regular_schedule():
                    print(f"Skipping irregular schedule {tap.schedule_id} with cron expression {tap.interval_mask}")
            else:
                taps.append(tap)
        except Exception as e:
            print(f"Skipping schedule {schedule_id} due to error: {e}")

    return taps

def _update_schedule_cron(tap : Tap):
    """
        Uses the start_time to shift the cron expression accordingly. Gene space is already limited from 0 to the frequency of the tap

        Ex. form of cron expression: "8-59/15 * * * *" (every 15 minutes starting at minute 8 of each hour)

        Args:
            tap (Tap): Tap object with updated shift attribute based on GA solution
        Returns:
            Updated tap object with new interval_mask based on the shift calculated by the GA optimizer
    """

    frequency = tap.frequency_minutes
    start_time_mins = tap.start_time_mins

    if tap.shifted == False or start_time_mins is None:
        return  # No shift needed
    
    if frequency == 1440:  # For daily taps, we can shift within the hour
        hour = start_time_mins // 60 
        minute = (start_time_mins - hour * 60) % 60
        tap.interval_mask = f"{minute} {hour} * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask} for tap {tap.schedule_id}")
        return
    elif frequency == 60:  # For hourly taps, we can shift within the hour
        assert start_time_mins < frequency, f"Shift {start_time_mins} cannot be greater than or equal to frequency {frequency} for tap {tap.schedule_id}"
        tap.interval_mask = f"{start_time_mins} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask} for tap {tap.schedule_id}")
        return
    elif frequency < 60:
        assert start_time_mins < frequency, f"Shift {start_time_mins} cannot be greater than or equal to frequency {frequency} for tap {tap.schedule_id}"
        tap.interval_mask = f"{start_time_mins}-59/{frequency} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(tap.interval_mask):
            raise ValueError(f"Invalid cron expression generated: {tap.interval_mask} for tap {tap.schedule_id}")
        return
        


def _assign_new_schedules(optimised_taps: list[pygad.Tap], db_cur):
    """Assign new schedules based on the optimal schedule found."""

    # For each tap, update the schedule in the DB with the new interval_mask based on the start_time_mins calculated by the GA optimizer
    for tap in optimised_taps:
        previous_schedule_mask = tap.interval_mask
        _update_schedule_cron(tap)
        if tap.shifted: print(f"- {tap.schedule_id} : {tap.interval_mask}")
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
        }
        scheduler.update_schedule_backups(db_cur, previous_schedule_details)


@utils.named_exception_handler("smart_schedule")
def main(server_id=None, dbname=None, ga_config=None):
    if server_id and type(server_id) != int: raise TypeError(f"server_id should be int not {type(server_id)}")

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    if not server_id:
        # Recursively call main for each server_id if no specific server_id is provided 
        server_ids = scheduler.get_all_server_ids(db_cur)
        for id in server_ids:
            main(server_id=id[0], dbname=dbname)
        
    else:
        # Get schedules for the server_id
        print("\n-----------------Tap Setup----------------------") 
        schedule_ids = _get_schedules_per_server(server_id=server_id, db_cur=db_cur)
        print(f"Found {len(schedule_ids)} schedules for server_id {server_id}")

        # Build Tap objects
        taps = _create_tap_objects(schedule_ids, db_cur=db_cur)
        if not taps:
            print("No valid schedules found to optimize.")
            sys.exit(1)
        print("-------------------------------------------------\n")
        

        try:
            print("\n------------Starting Optimisation-----------------") 
            blacklist_schedule_ids = scheduler.get_blacklisted_schedule_ids(db_cur)
            print(f"Blacklisted schedule IDs that will be excluded from optimization: {blacklist_schedule_ids}")
            ga = pygad.GAPyGADScheduler(config=ga_config, blacklist_schedule_ids=blacklist_schedule_ids)
            print("Running PyGAD solver ...")
            optimised_taps, __, peak_cpu, __, initial_fitness = ga.solve(taps)
            print(f"Optimized schedule for server_id {server_id}: new peak CPU {peak_cpu}")
            print("--------------------------------------------------\n")


            print("\n-------------Updating Schedules------------------") 
            if peak_cpu < initial_fitness:  # Only update schedules if we have found an improvement
                _assign_new_schedules(optimised_taps, db_cur=db_cur)
            else:
                print(f"No improvement found for server_id {server_id}. Current peak CPU: {initial_fitness}, Optimized peak CPU: {peak_cpu}. No schedule updates will be made.")
            print("--------------------------------------------------\n")

        except Exception as e:
            print(f"Error during optimization for server_id {server_id}: {e}")
            sys.exit(1)

    db_cur.close()
    db_conn.close()