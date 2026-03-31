"""Shifts the schedules on a node to distribute the load"""

import datetime
import sys
# from typing import Optional
# from croniter import croniter
from cicada.lib import postgres, utils
from cicada.lib import scheduler
# from cicada.lib.SmartScheduling import pygad
# from cicada.lib.SmartScheduling.domain import Tap


def get_schedules_per_server(server_id, dbname=None):
    """Get all schedules for a given server_id."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
        # Don't get the schedules that aren't taps -> schedule_description LIKE '%==%' ?          !?
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


# def create_tap_objects(schedule_ids, dbname=None):
    """Create Tap objects from schedule_ids."""
    
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    taps : list[pygad.Tap] = []

    # Fetch details for each schedule and convert to Tap objects
    for schedule_id in schedule_ids:
        details = scheduler.get_schedule_details(db_cur, schedule_id)
        try:
            tap = pygad.Tap(
                schedule_id=schedule_id,
                interval_mask=details.get("interval_mask")
            )

            # Ignore the few taps that have irregular cron expressions for now. There are few enough that this shouldn't impact the optimisation and is not worth the effort to try and support these irregular schedules in the GA
            if not tap.is_regular_schedule(tap.interval_mask):
                raise ValueError(f"Skipping irregular cron expression: {tap.interval_mask}")
            else:
                tap.determine_attributes(db_cur)
                taps.append(tap)

        except Exception as e:
            print(f"Skipping schedule {schedule_id} due to error: {e}")

    db_cur.close()
    db_conn.close()
    return taps

# def update_schedule_cron(tap : Tap) -> str:
    """
        Uses the start_blocks to shift the cron expression accordingly. Gene space is already limited from 0 to the frequency of the tap

        Ex. form of cron expression: "8-59/15 * * * *" (every 15 minutes starting at minute 8 of each hour)
    """
    frequency = tap.frequency_minutes
    shift = tap.shift 

    if not shift or frequency >= 60:
        return tap  # No shift needed
    
    if shift == 0:
        return tap  # No shift needed
    if shift < 60:
        if frequency >= 60:
            minute = shift % 60
            hour = shift // 60 + croniter(tap.interval_mask).get_next(datetime).hour  # Get the hour of the first scheduled run and add the shift in hours
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
    # Do we only want to support shifts of less than an hour? --> most schedules are on a 30 minute basis and it would simplify this function           !
    if shift >= 60:
        raise NotImplementedError("Design decision needed on how to implement schedule shifting for different frequencies and shift amounts.")
    
    

# def assign_new_schedules(optimised_taps: list[pygad.Tap], dbname=None):
    """Assign new schedules based on the optimal schedule found."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()

    # For each tap, update the schedule in the DB with the new interval_mask based on the shift calculated by the GA optimizer
    for tap in optimised_taps:
        tap = update_schedule_cron(tap)
        schedule_details = {
            "schedule_id": tap.schedule_id,
            "interval_mask": tap.interval_mask
        }  
        scheduler.update_schedule_details(db_cur=db_cur, schedule_details=schedule_details)
    db_conn.commit()
    db_cur.close()
    db_conn.close()

# def rollback(tap : Optional[Tap], dbname=None):
    """
    Rollback to original schedules in case of any issues during assignment.
    Args:        tap: Optional[Tap] : the tap to rollback, if None rollback all taps to their original schedules

    """
    # This would require storing the original schedules before making changes, which is not currently implemented. 
    # We could potentially store the original interval_masks in a separate table or in memory before updating, and then use that for rollback if needed.
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

    # # Build Tap objects
    # taps = create_tap_objects(schedule_ids, dbname=dbname)
    # if not taps:
    #     print("No valid schedules found to optimize.")
    #     sys.exit(1)


    # # Run GA optimizer ---> add in way to change GAConfig parameters    !
    # try:
    #     ga = pygad.GAPyGADScheduler()
    #     optimised_taps = ga.solve(taps)

    #                     # Add logic to actually assign the new schedules    !
    #     assign_new_schedules(optimised_taps, dbname=dbname)

    # except Exception as e:
    #     print(f"Error during optimization for server_id {server_id}: {e}")
    #     sys.exit(1)

