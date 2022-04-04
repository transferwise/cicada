"""Execute a using schedule_id."""

import datetime
import subprocess
import psutil
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

    sqlquery = (
        f"UPDATE schedule_log SET end_time = now() ,returncode = {str(returncode)}"
    )

    if error_detail:
        sqlquery += f",error_detail = '{str(error_detail)}'"

    sqlquery += f" WHERE schedule_log_id = '{str(schedule_log_id)}'"

    db_cur.execute(sqlquery)


def send_slack_error(schedule_id, schedule_log_id, returncode, error_detail):
    """send_slack_error"""
    utils.send_slack_message(
        f":exclamation: *ERROR* schedule_id `{schedule_id}` execution failure",
        f"```"
        f"server time     : {datetime.datetime.now()}\n"
        f"schedule_log_id : {schedule_log_id}\n"
        f"returncode      : {returncode}\n"
        f"error_detail    : {error_detail}```",
        "danger",
    )


def catch_sigterm(signum, frame):
    """catch_sigterm"""
    raise OSError(-15, "SIGTERM received")


def catch_sigquit(signum, frame):
    """catch_sigquit"""
    raise OSError(-15, "SIGQUIT received")


@utils.named_exception_handler("exec_schedule")
# pylint: disable=too-many-locals
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
def main(schedule_id, dbname=None):
    """Execute a using schedule_id."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    server_id = scheduler.get_server_id(db_cur)

    # Get schedule details and execute
    obj_schedule_details = scheduler.get_schedule_executable(db_cur, schedule_id)

    # pylint: disable=too-many-nested-blocks
    for row in obj_schedule_details.fetchall():
        command = str(row[0])
        parameters = str(row[1])

        full_command = []
        full_command.extend(command.split())
        full_command.extend(parameters.split())

        human_full_command = str(" ".join(full_command))

        # Check to see that schedule is not already running
        if get_is_running(db_cur, schedule_id) == 0:
            # Initiate schedule log
            schedule_log_id = init_schedule_log(
                db_cur, server_id, schedule_id, human_full_command
            )
            reset_adhoc_details(db_cur, schedule_id)

            set_is_running(db_cur, schedule_id)

            signal.signal(signal.SIGTERM, catch_sigterm)
            signal.signal(signal.SIGQUIT, catch_sigquit)

            cicada_pid = os.getpid()

            error_detail = None
            returncode = None

            try:
                # pylint: disable=consider-using-with
                child_process = subprocess.Popen(
                    full_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )

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
                            db_cur.close()
                            db_conn.close()
                        # pylint: disable=unused-variable
                        except Exception as error:
                            send_slack_error(
                                schedule_id,
                                schedule_log_id,
                                returncode,
                                "Cicada database not reachable",
                            )
                            time.sleep(1)

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

                # Terminate any zombie processes
                for zombie in psutil.Process(cicada_pid).children(recursive=True):
                    zombie.send_signal(signal.SIGTERM)

                # Repeatedly attempt to finalize schedule, even if db is unavailable
                db_connection_made = False
                while not db_connection_made:
                    try:
                        db_conn = postgres.db_cicada(dbname)
                        db_cur = db_conn.cursor()
                        db_connection_made = True
                    except Exception:
                        send_slack_error(
                            schedule_id,
                            schedule_log_id,
                            returncode,
                            "Cicada database not reachable",
                        )
                        time.sleep(1)

                unset_is_running(db_cur, schedule_id)
                finalize_schedule_log(db_cur, schedule_log_id, returncode, error_detail)

    db_cur.close()
    db_conn.close()
