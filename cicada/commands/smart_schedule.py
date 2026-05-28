"""Shifts the schedules on a node to distribute the load"""

from __future__ import annotations
import sys
from typing import List
from croniter import croniter
from cicada.lib import postgres, utils
from cicada.lib import scheduler
from cicada.lib.smart_scheduling.GAPyGAD import GAPyGADScheduler
from cicada.lib.smart_scheduling.domain import Schedule

def _get_schedules_per_server(server_id, db_cur=None):
    """Get all schedules for a given server_id."""
    schedule_ids = [row[0] for row in scheduler.get_all_schedule_ids_per_server(db_cur, server_id)]

    if not schedule_ids:
        raise ValueError(f"No schedules found for server_id {server_id}. Check if server_id exists and has schedules assigned.")

    return schedule_ids



def _create_schedule_objects(schedule_ids, db_cur):
    """Create Schedule objects from schedule_ids."""
    
    schedules : list[Schedule] = []
    blocklisted_schedules = scheduler.get_blocklisted_schedule_ids(db_cur)

    # Fetch details for each schedule and convert to Schedule objects
    for schedule_id in schedule_ids:
        details = scheduler.get_schedule_details(db_cur, schedule_id)
        if schedule_id in blocklisted_schedules:
            details['blocklisted'] = True
        else:
            details['blocklisted'] = False

        try:
            schedule = Schedule(
                schedule_id = details['schedule_id'],
                server_id = details['server_id'],
                interval_mask = details['interval_mask'],
                smart_interval_mask = details.get('smart_interval_mask'),
                blocklisted = details.get('blocklisted', False),
                db_cur = db_cur
            )
            # Ignore the few schedules that have irregular cron expressions for now. 
            # There are few enough that this shouldn't impact the optimisation and is not worth the effort to try and support these in the GA
            if not schedule.is_regular_schedule():
                print(f"Skipping irregular schedule {schedule.schedule_id} with cron expression {schedule.interval_mask}")
            else:
                schedules.append(schedule)
        except Exception as e:
            print(f"Skipping schedule {schedule_id} due to error: {e}")

    return schedules

def _update_schedule_cron(schedule : Schedule):
    """
        Uses the start_time to shift the cron expression accordingly. Gene space is already limited from 0 to the frequency of the schedule

        Ex. form of cron expression: "8-59/15 * * * *" (every 15 minutes starting at minute 8 of each hour)

        Args:
            schedule (Schedule): Schedule object with updated shift attribute based on GA solution
        Returns:
            Updated Schedule object with new interval_mask based on the shift calculated by the GA optimizer
    """

    frequency = schedule.frequency_minutes
    start_time_mins = schedule.start_time_mins

    if schedule.shifted == False or start_time_mins is None:
        return  # No shift needed
    
    if frequency == 1440:  # For daily schedules, we can shift within the hour
        hour = start_time_mins // 60 
        minute = (start_time_mins - hour * 60) % 60
        schedule.smart_interval_mask = f"{minute} {hour} * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(schedule.smart_interval_mask):
            raise ValueError(f"Invalid cron expression generated: {schedule.smart_interval_mask} for schedule {schedule.schedule_id}")
        return
    elif frequency == 60:  # For hourly schedules, we can shift within the hour
        if start_time_mins >= frequency:
            raise ValueError(f"Shift {start_time_mins} cannot be greater than or equal to frequency {frequency} for schedule {schedule.schedule_id}")
        schedule.smart_interval_mask = f"{start_time_mins} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(schedule.smart_interval_mask):
            raise ValueError(f"Invalid cron expression generated: {schedule.smart_interval_mask} for schedule {schedule.schedule_id}")
        return
    elif frequency < 60:
        if start_time_mins >= frequency:
            raise ValueError(f"Shift {start_time_mins} cannot be greater than or equal to frequency {frequency} for schedule {schedule.schedule_id}")
        schedule.smart_interval_mask = f"{start_time_mins}-59/{frequency} * * * *"
        # Check that the new cron expression is valid
        if not croniter.is_valid(schedule.smart_interval_mask):
            raise ValueError(f"Invalid cron expression generated: {schedule.smart_interval_mask} for schedule {schedule.schedule_id}")
        return
        


def _assign_new_schedules(optimised_schedules: List[Schedule], db_cur):
    """Assign new schedules based on the optimal schedule found."""

    schedule_details_list = []
    schedule_ids = []
    
    # For each schedule, update the schedule in the DB with the new interval_mask based on the start_time_mins calculated by the GA optimizer
    for schedule in optimised_schedules:
        _update_schedule_cron(schedule)
        if schedule.shifted:
            print(f"- {schedule.schedule_id} : {schedule.smart_interval_mask}")
            schedule._determine_start_time_mins() 

            schedule_details = {
                "adhoc_parameters": None,
                "adhoc_execute": None,
                "schedule_group_id": None,
                "parameters": None,
                "server_id": None,
                "last_run_date": None,
                "is_enabled": None,
                "interval_mask": None,
                "schedule_description": None,
                "auto_update_time": None,
                "schedule_order": None,
                "schedule_id": schedule.schedule_id,
                "is_async": None,
                "abort_running": None,
                "exec_command": None,
                "first_run_date": None,
                "is_running": None,
                "smart_interval_mask": schedule.smart_interval_mask
            }
            schedule_details_list.append(schedule_details)
            schedule_ids.append(schedule.schedule_id)

    scheduler.update_schedule_details_bulk(db_cur=db_cur, schedule_list=schedule_details_list)


@utils.named_exception_handler("smart_schedule")
def main(server_id=None, dbname=None, ga_config=None):
    if server_id and type(server_id) != int: raise TypeError(f"server_id should be int not {type(server_id)}")

    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    optimise(db_cur=db_cur, server_id=server_id, ga_config=ga_config)
    db_cur.close()
    db_conn.close()


def optimise(db_cur, server_id=None, ga_config=None):
    if not server_id:
        # Recursively call main for each server_id if no specific server_id is provided 
        server_ids = scheduler.get_all_server_ids(db_cur)
        for id in server_ids:
            optimise(db_cur=db_cur, server_id=id[0], ga_config=ga_config)
        
    else:

        if not scheduler.validate_server_id(db_cur, server_id=server_id): 
            raise ValueError(f"Server with server_id={server_id} does not exist in the database")
        
        # Get schedules for the server_id
        print("\n-----------------Schedule Setup----------------------") 
        # Prevents process from progressing if no schedules are found for the server_id 
        # however allows optimisation to still run for other server_ids if running for all servers (server_id=None)
        try:
            schedule_ids = _get_schedules_per_server(server_id=server_id, db_cur=db_cur)
        except ValueError as e:
            print(e)
            return
        print(f"Found {len(schedule_ids)} schedules for server_id {server_id}")

        # Build schedule objects
        schedules = _create_schedule_objects(schedule_ids, db_cur=db_cur)
        if not schedules:
            raise ValueError(f"No valid schedules found to optimize for server_id {server_id}")
        print("-------------------------------------------------\n")
        

        try:
            print("\n------------Starting Optimisation-----------------") 
            print("Running PyGAD solver ...")
            ga = GAPyGADScheduler(config=ga_config)
            optimised_schedules, __, peak_usage, __, initial_fitness = ga.solve(schedules)
            print(f"Optimized schedule for server_id {server_id}: new peak usage {peak_usage}")


            if peak_usage < initial_fitness:  
                try:
                    print("\n-------------Updating Schedules------------------") 
                    db_cur.execute("BEGIN;")
                    _assign_new_schedules(optimised_schedules, db_cur=db_cur)
                    scheduler.snapshot_schedules(db_cur, server_id=server_id, computed_usage=peak_usage, reason='Smart Schedule Optimization')
                    db_cur.execute("COMMIT;")
                except Exception as e:
                    db_cur.execute("ROLLBACK;")
                    print("Database changes have been rolled back due to the error.")
                    raise Exception(f"Error during schedule update for server_id {server_id}: {e}")
                print("--------------------------------------------------\n")
            else:
                print(f"No improvement found for server_id {server_id}. Current peak usage: {initial_fitness}, Optimized peak usage: {peak_usage}. No schedule updates will be made.")
                print("--------------------------------------------------\n")

        except Exception as e:
            print(f"Error during optimization for server_id {server_id}: {e}")
            sys.exit(1)
