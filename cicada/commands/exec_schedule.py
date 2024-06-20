"""Execute a using schedule_id."""

import datetime
import subprocess
import os
import signal
import time
import uuid

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


def get_is_running(db_cur, schedule_id):
    """Get is_running status"""
    sqlquery = f"""
    SELECT is_running
    FROM schedules
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)
    row = db_cur.fetchone()
    is_running = row[0]

    return is_running


def set_is_running(db_cur, schedule_id):
    """Set is_running status"""
    sqlquery = f"""
    UPDATE schedules
    SET is_running = 1
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)


def unset_is_running(db_cur, schedule_id):
    """Unset is_running status"""
    sqlquery = f"""
    UPDATE schedules
    SET is_running = 0
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)


def reset_adhoc_details(db_cur, schedule_id):
    """Reset ad-hoc details"""
    sqlquery = f"""
    UPDATE schedules SET
        adhoc_execute = 0,
        adhoc_parameters = NULL
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)


def get_abort_running(db_cur, schedule_id):
    """Get abort_running status"""
    sqlquery = f"""
    SELECT abort_running
    FROM schedules
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)
    row = db_cur.fetchone()
    abort_running = row[0]

    if abort_running == 0:
        return False

    return True


def unset_abort_running(db_cur, schedule_id):
    """unset_abort_running"""
    sqlquery = f"""
    UPDATE schedules SET
        abort_running = 0
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)


def init_schedule_log(db_cur, server_id, schedule_id, full_command):
    """Initialise a schedule log"""
    # Get local machine uuid
    schedule_log_id = uuid.uuid1()

    full_command = postgres.escape_upsert_string(full_command)

    sqlquery = f"""
    INSERT INTO schedule_log
        (schedule_log_id, server_id, schedule_id, full_command, start_time)
    VALUES
        ('{str(schedule_log_id)}', {str(server_id)}, '{str(schedule_id)}', '{str(full_command)}', now())
    """
    db_cur.execute(sqlquery)

    return schedule_log_id


def finalize_schedule_log(db_cur, schedule_log_id, returncode, error_detail):
    """Finalize a schedule log"""

    sqlquery = f"UPDATE schedule_log SET end_time = now() ,returncode = {str(returncode)}"

    if error_detail:
        sqlquery += f",error_detail = '{str(error_detail)}'"

    sqlquery += f" WHERE schedule_log_id = '{str(schedule_log_id)}'"

    db_cur.execute(sqlquery)


def send_slack_error(schedule_id, schedule_log_id, returncode, description, error):
    """send_slack_error"""
    utils.send_slack_message(
        f":exclamation: *ERROR* schedule_id `{schedule_id}` execution failure",
        f"```"
        f"server utc time : {datetime.datetime.utcnow()}\n"
        f"schedule_log_id : {schedule_log_id}\n"
        f"returncode      : {returncode}\n"
        f"description     : {description}\n"
        f"\n"
        f"error           : {error}"
        f"```",
        "danger",
    )


def catch_sigterm(signum, frame):
    """catch_sigterm"""
    raise OSError(-15, "SIGTERM received")


def catch_sigquit(signum, frame):
    """catch_sigquit"""
    raise OSError(-15, "SIGQUIT received")


@utils.named_exception_handler("exec_schedule")
def main(schedule_id, dbname=None):
    """Execute a using schedule_id."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    server_id = scheduler.get_server_id(db_cur)

    # Get schedule details and execute
    obj_schedule_details = scheduler.get_schedule_executable(db_cur, schedule_id)

    row = obj_schedule_details.fetchone()
    command = str(row[0])
    parameters = str(row[1])

    full_command = []
    full_command.extend(command.split())
    full_command.extend(parameters.split())

    human_full_command = str(" ".join(full_command))

    # Check to see that schedule is not already running
    if get_is_running(db_cur, schedule_id) == 0:
        # Initiate schedule log
        schedule_log_id = init_schedule_log(db_cur, server_id, schedule_id, human_full_command)
        reset_adhoc_details(db_cur, schedule_id)

        set_is_running(db_cur, schedule_id)

        db_cur.close()
        db_conn.close()

        signal.signal(signal.SIGTERM, catch_sigterm)
        signal.signal(signal.SIGQUIT, catch_sigquit)

        error_detail = None
        returncode = None

        db_conn_alert_delay = 15
        db_conn_alert_next = datetime.datetime.utcnow() + datetime.timedelta(minutes=db_conn_alert_delay)

        try:
            child_process = subprocess.Popen(full_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            # Check if child process has terminated

            while returncode is None:
                time.sleep(1)
                returncode = child_process.poll()

                # If still running, check if child_process should be aborted
                if returncode is None:
                    # protect against db unavailable
                    try:
                        db_conn = postgres.db_cicada(dbname)
                        db_cur = db_conn.cursor()
                        if get_abort_running(db_cur, schedule_id):
                            # Terminate main process
                            returncode = -15
                            error_detail = "Cicada abort_running"
                            unset_abort_running(db_cur, schedule_id)
                            child_process.terminate()

                        db_cur.close()
                        db_conn.close()
                    except Exception as error:
                        if datetime.datetime.utcnow() >= db_conn_alert_next:
                            send_slack_error(
                                schedule_id,
                                schedule_log_id,
                                returncode,
                                f"Cicada db unavailable - check abort_running - {db_conn_alert_delay} minutes",
                                error,
                            )
                            db_conn_alert_next = datetime.datetime.utcnow() + datetime.timedelta(
                                minutes=db_conn_alert_delay
                            )
                        time.sleep(5)
                    else:
                        db_conn_alert_next = datetime.datetime.utcnow() + datetime.timedelta(
                            minutes=db_conn_alert_delay
                        )

            if returncode != 0:
                config = utils.load_config()
                returncodes_alert = config["slack"].get("returncodes_alert", "*")

                if returncodes_alert == "*" or returncode in returncodes_alert:
                    send_slack_error(
                        schedule_id,
                        schedule_log_id,
                        returncode,
                        None,
                        None,
                    )

        # Capture error
        except OSError as error:
            returncode = error.errno
            error_detail = error.strerror
        except subprocess.CalledProcessError as error:
            returncode = error.returncode
            error_detail = "CalledProcessError"
        except KeyboardInterrupt:
            returncode = 1
            error_detail = "KeyboardInterrupt"
        except SystemExit:
            returncode = 1
            error_detail = "SystemExit"
        except Exception:
            returncode = 999
            error_detail = "Crazy Unknown Error"
        finally:
            # Repeatedly attempt to finalize schedule, even if db is unavailable
            while True:
                try:
                    db_conn = postgres.db_cicada(dbname)
                    db_cur = db_conn.cursor()
                    break
                except Exception as error:
                    if datetime.datetime.utcnow() >= db_conn_alert_next:
                        send_slack_error(
                            schedule_id,
                            schedule_log_id,
                            returncode,
                            f"Cicada db unavailable - finalize schedule - {db_conn_alert_delay} minutes",
                            error,
                        )
                        db_conn_alert_next = datetime.datetime.utcnow() + datetime.timedelta(
                            minutes=db_conn_alert_delay
                        )
                    time.sleep(5)

            unset_is_running(db_cur, schedule_id)
            finalize_schedule_log(db_cur, schedule_log_id, returncode, error_detail)

    db_cur.close()
    db_conn.close()
