"""Execute a using schedule_id."""

import datetime
import subprocess
import time

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
    else:
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
    # Get UUID
    sqlquery = """
    SELECT uuid_generate_v1()
    """
    db_cur.execute(sqlquery)
    row = db_cur.fetchone()
    schedule_log_id = str(row[0])

    sqlquery = f"""
    INSERT INTO schedule_log
        (schedule_log_id, server_id, schedule_id, full_command, start_time, schedule_log_status_id)
    VALUES
        ('{str(schedule_log_id)}', {str(server_id)}, '{str(schedule_id)}', '{str(full_command)}', now() ,1)
    """
    db_cur.execute(sqlquery)

    return schedule_log_id


def finalize_schedule_log(db_cur, schedule_log_id, returncode, error_detail):
    """Finalize a schedule log"""
    if returncode == 0:
        schedule_log_status_id = 2
    else:
        schedule_log_status_id = 3

    sqlquery = f"""
    UPDATE schedule_log SET
        end_time = now(),
        returncode = {str(returncode)},
    """

    if error_detail:
        sqlquery += f"error_detail = '{str(error_detail)}',"

    sqlquery += f"""
        schedule_log_status_id = {str(schedule_log_status_id)}
    WHERE schedule_log_id = '{str(schedule_log_id)}'
    """

    db_cur.execute(sqlquery)


def send_slack_error(schedule_id, schedule_log_id, returncode, error_detail):
    """send_slack_error"""
    utils.send_slack_message(f":exclamation: *ERROR* schedule_id `{schedule_id}` failed to execute",
            f"```" \
            f"server time     : {datetime.datetime.now()}\n" \
            f"schedule_log_id : {schedule_log_id}\n" \
            f"returncode      : {returncode}\n" \
            f"error_detail    : {error_detail}```",
            'danger')


@utils.named_exception_handler('exec_schedule')
def main(schedule_id, dbname = None):
    """Execute a using schedule_id."""
    db_conn = postgres.db_cicada(dbname)
    db_cur = db_conn.cursor()
    server_id = scheduler.get_server_id(db_cur)

    # Get schedule details and execute
    obj_schedule_details = scheduler.get_schedule_executable(db_cur, schedule_id)

    for row in obj_schedule_details.fetchall():
        command = str(row[0])
        parameters = str(row[1])

        full_command = []
        full_command.extend(command.split())
        full_command.extend(parameters.split())

        human_full_command = str(' '.join(full_command))

        # Check to see that schedule is not already running
        if get_is_running(db_cur, schedule_id) == 0:
            # Initiate schedule log
            schedule_log_id = init_schedule_log(db_cur, server_id, schedule_id, human_full_command)
            reset_adhoc_details(db_cur, schedule_id)
            set_is_running(db_cur, schedule_id)

            try:
                running_process = subprocess.Popen(full_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                error_detail = None
                # Continually monitor to see if process is still running
                while running_process.poll() is None:
                    # Check to see if process should be killed,
                    # But don't bomb if db is not available
                    try:
                        db_conn = postgres.db_cicada(dbname)
                        db_cur = db_conn.cursor()
                        if get_abort_running(db_cur, schedule_id):
                            # https://docs.python.org/3.8/library/subprocess.html#subprocess.Popen.terminate
                            running_process.terminate()
                            error_detail = 'SIGTERM by Cicada'
                            unset_abort_running(db_cur, schedule_id)
                        db_cur.close()
                        db_conn.close()
                    except:
                        pass

                    time.sleep(1)
                returncode = running_process.returncode

            # Capture error
            except OSError as error:
                returncode = error.errno
                error_detail = error.strerror
            except subprocess.CalledProcessError as error:
                returncode = error.returncode
                error_detail = error.output
            except Exception as error:
                returncode = 9
                error_detail = 'Crazy Unknown Error'
            finally:
                if (returncode != 0) and (error_detail != 'SIGTERM by Cicada'):
                    send_slack_error(schedule_id, schedule_log_id, returncode, error_detail)

                # Repeatedly attempt to finalize schedule, even if db is unavailable
                db_connection_made = False
                while not db_connection_made:
                    try:
                        db_conn = postgres.db_cicada(dbname)
                        db_cur = db_conn.cursor()
                        db_connection_made = True
                    except Exception:
                        time.sleep(1)

                unset_is_running(db_cur, schedule_id)
                finalize_schedule_log(db_cur, schedule_log_id, returncode, error_detail)

    db_cur.close()
    db_conn.close()
