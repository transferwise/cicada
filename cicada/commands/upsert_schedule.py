"""Upsert a schedule using schedule_id."""

import sys

from tabulate import tabulate
from croniter import croniter

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


@utils.named_exception_handler("upsert_schedules")
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
def main(schedule_details, dbname=None):
    """Upsert a schedule using schedule_id."""

    if schedule_details["schedule_id"] is None:
        print("ERROR: schedule_id is required")
        sys.exit(1)

    if " " in str(schedule_details["schedule_id"]):
        print("ERROR: schedule_id cannot contain blank spaces")
        sys.exit(1)

    # Get schedule details
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    current_schedule_details = scheduler.get_schedule_details(
        db_cur, schedule_details["schedule_id"]
    )

    if not current_schedule_details:

        if schedule_details["interval_mask"] is None:
            print("ERROR: interval_mask is required for new schedule")
            sys.exit(1)

        if not croniter.is_valid(schedule_details["interval_mask"]):
            print("ERROR: interval_mask is invalid")
            sys.exit(1)

        if schedule_details["exec_command"] is None:
            print("ERROR: exec_command is required for a new schedule")
            sys.exit(1)

        if schedule_details["schedule_order"] is None:
            schedule_details["schedule_order"] = 1

        new_schedule_details = schedule_details.copy()

        scheduler.insert_schedule_details(db_cur, new_schedule_details)

    else:
        # existing current_schedule_details = existing Schedule
        new_schedule_details = current_schedule_details.copy()

        if schedule_details["schedule_description"] is not None:
            new_schedule_details["schedule_description"] = schedule_details[
                "schedule_description"
            ]
        if schedule_details["server_id"] is not None:
            new_schedule_details["server_id"] = schedule_details["server_id"]
        if schedule_details["schedule_order"] is not None:
            new_schedule_details["schedule_order"] = schedule_details["schedule_order"]
        if schedule_details["is_async"] is not None:
            new_schedule_details["is_async"] = schedule_details["is_async"]
        if schedule_details["is_enabled"] is not None:
            new_schedule_details["is_enabled"] = schedule_details["is_enabled"]
        if schedule_details["adhoc_execute"] is not None:
            new_schedule_details["adhoc_execute"] = schedule_details["adhoc_execute"]
        if schedule_details["interval_mask"] is not None:
            new_schedule_details["interval_mask"] = schedule_details["interval_mask"]
        if schedule_details["first_run_date"] is not None:
            new_schedule_details["first_run_date"] = schedule_details["first_run_date"]
        if schedule_details["last_run_date"] is not None:
            new_schedule_details["last_run_date"] = schedule_details["last_run_date"]
        if schedule_details["exec_command"] is not None:
            new_schedule_details["exec_command"] = schedule_details["exec_command"]
        if schedule_details["parameters"] is not None:
            new_schedule_details["parameters"] = schedule_details["parameters"]
        if schedule_details["adhoc_parameters"] is not None:
            new_schedule_details["adhoc_parameters"] = schedule_details[
                "adhoc_parameters"
            ]
        if schedule_details["schedule_group_id"] is not None:
            new_schedule_details["schedule_group_id"] = schedule_details[
                "schedule_group_id"
            ]

        scheduler.update_schedule_details(db_cur, new_schedule_details)

    print(tabulate(new_schedule_details.items(), ["Detail", "Value"], tablefmt="psql"))
    db_cur.close()
    db_conn.close()
