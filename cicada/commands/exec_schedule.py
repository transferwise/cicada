"""Execute a using schedule_id."""

import datetime
import subprocess
import signal
import threading
import time
import uuid
from typing import NamedTuple, Optional

from cicada.lib import postgres
from cicada.lib import scheduler
from cicada.lib import utils


DB_ALERT_DELAY_MINUTES = 15
DB_RETRY_DELAY_SECONDS = 5
CHILD_WAIT_TIMEOUT_SECONDS = 1
CHILD_STDERR_CAPTURE_MAX_BYTES = 4096
CHILD_STDERR_DRAIN_TIMEOUT_SECONDS = 1
UNKNOWN_RETURN_CODE = 999
ERROR_DETAIL_MAX_LENGTH = 255


class ExecutionResult(NamedTuple):
    """A child execution outcome recorded in schedule_log."""

    returncode: int
    error_detail: Optional[str] = None


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
    """Atomically clear the running state and any outstanding abort request."""
    sqlquery = """
    UPDATE schedules
    SET is_running = 0,
        abort_running = 0
    WHERE schedule_id = %s"""

    db_cur.execute(sqlquery, (schedule_id,))


def reset_adhoc_details(db_cur, schedule_id):
    """Reset ad-hoc details"""
    sqlquery = f"""
    UPDATE schedules SET
        adhoc_execute = 0,
        adhoc_parameters = NULL
    WHERE schedule_id = '{str(schedule_id)}'"""

    db_cur.execute(sqlquery)


def consume_abort_running(dbname, schedule_id):
    """Atomically reset and report an outstanding abort request."""
    with postgres.db_cicada_cursor(dbname) as (_, db_cur):
        db_cur.execute(
            """
            UPDATE schedules
            SET abort_running = 0
            WHERE schedule_id = %s
              AND abort_running = 1
            RETURNING schedule_id
            """,
            (schedule_id,),
        )
        return db_cur.fetchone() is not None


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
    if returncode is None:
        returncode = UNKNOWN_RETURN_CODE
    if error_detail is not None:
        error_detail = str(error_detail)[:ERROR_DETAIL_MAX_LENGTH]

    db_cur.execute(
        """
        UPDATE schedule_log
        SET end_time = now(),
            returncode = %s,
            error_detail = %s
        WHERE schedule_log_id = %s
        """,
        (returncode, error_detail, str(schedule_log_id)),
    )


def send_slack_error(schedule_id, server_id, interval_mask, schedule_log_id, returncode, context, error_detail):
    """Send a schedule execution error with its diagnostic context."""
    details = (
        f"```"
        f"server utc time : {datetime.datetime.utcnow()}\n"
        f"schedule_log_id : {schedule_log_id}\n"
        f"server_id       : {server_id}\n"
        f"interval_mask   : {interval_mask}\n"
        f"returncode      : {returncode}\n"
        f"error_detail    : {error_detail}"
    )
    if context is not None:
        details += f"\ncontext         : {context}"
    details += "```"

    utils.send_slack_message(
        f":exclamation: *ERROR* schedule_id `{schedule_id}` execution failure",
        details,
        "danger",
    )


def next_db_alert_time(delay_minutes=DB_ALERT_DELAY_MINUTES):
    """Return the next time a persistent database failure should alert."""
    return datetime.datetime.utcnow() + datetime.timedelta(minutes=delay_minutes)


def handle_db_unavailable(
    schedule_id,
    server_id,
    interval_mask,
    schedule_log_id,
    returncode,
    operation,
    error,
    alert_next,
    alert_delay=DB_ALERT_DELAY_MINUTES,
):
    """Rate-limit a consistently formatted database outage alert."""
    now = datetime.datetime.utcnow()
    if now >= alert_next:
        send_slack_error(
            schedule_id,
            server_id,
            interval_mask,
            schedule_log_id,
            returncode,
            f"Cicada db unavailable - {operation} - {alert_delay} minutes",
            error,
        )
        alert_next = now + datetime.timedelta(minutes=alert_delay)

    time.sleep(DB_RETRY_DELAY_SECONDS)
    return alert_next


def consume_abort_running_with_retry(
    dbname,
    schedule_id,
    server_id,
    interval_mask,
    schedule_log_id,
    returncode,
    alert_next,
):
    """Consume an abort request while keeping database outage handling uniform."""
    try:
        abort_requested = consume_abort_running(dbname, schedule_id)
    except Exception as error:
        alert_next = handle_db_unavailable(
            schedule_id,
            server_id,
            interval_mask,
            schedule_log_id,
            returncode,
            "consume abort_running",
            error,
            alert_next,
        )
        return False, alert_next

    return abort_requested, next_db_alert_time()


def terminate_child_process(child_process):
    """Request direct-child termination and report whether the signal was accepted."""
    try:
        child_process.terminate()
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return True


def poll_child_returncode(child_process):
    """Return the child code, or None if it is still running or polling fails."""
    try:
        return child_process.poll()
    except Exception:
        return None


def execution_result_from_exception(error):
    """Map an execution exception to the existing schedule_log result."""
    if isinstance(error, subprocess.CalledProcessError):
        return ExecutionResult(error.returncode, "CalledProcessError")
    if isinstance(error, OSError):
        return ExecutionResult(
            error.errno if error.errno is not None else UNKNOWN_RETURN_CODE,
            error.strerror or str(error) or error.__class__.__name__,
        )
    if isinstance(error, KeyboardInterrupt):
        return ExecutionResult(1, "KeyboardInterrupt")
    if isinstance(error, SystemExit):
        return ExecutionResult(1, "SystemExit")
    return ExecutionResult(UNKNOWN_RETURN_CODE, str(error) or error.__class__.__name__)


def capture_stream_tail(stream, captured, max_bytes=CHILD_STDERR_CAPTURE_MAX_BYTES):
    """Drain a binary stream while retaining only its most recent bytes."""
    try:
        while True:
            chunk = stream.read(1024)
            if not chunk:
                return
            captured.extend(chunk)
            if len(captured) > max_bytes:
                del captured[:-max_bytes]
    except (OSError, ValueError):
        return
    finally:
        stream.close()


def stderr_error_detail(captured):
    """Return a bounded child error suitable for schedule_log and Slack."""
    detail = bytes(captured).decode("utf-8", errors="replace").strip()
    if not detail:
        return None
    return detail[-ERROR_DETAIL_MAX_LENGTH:]


def supervise_child_process(
    child_process,
    shutdown_request,
    dbname,
    schedule_id,
    server_id,
    interval_mask,
    schedule_log_id,
    alert_next,
):
    """Wait for one child to exit while handling termination requests."""
    termination_sent = False
    requested_stop_result = None
    supervision_error_detail = None

    def confirmed_exit_result(child_returncode):
        if requested_stop_result is not None:
            return requested_stop_result
        return ExecutionResult(child_returncode, supervision_error_detail)

    while True:
        try:
            if (requested_stop_result is not None or supervision_error_detail is not None) and not termination_sent:
                termination_sent = terminate_child_process(child_process)

            try:
                child_returncode = child_process.wait(timeout=CHILD_WAIT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                pass
            except OSError:
                child_returncode = poll_child_returncode(child_process)
                if child_returncode is not None:
                    return confirmed_exit_result(child_returncode), alert_next
                time.sleep(CHILD_WAIT_TIMEOUT_SECONDS)
            except Exception as error:
                if supervision_error_detail is None:
                    supervision_error_detail = execution_result_from_exception(error).error_detail
                if not termination_sent:
                    termination_sent = terminate_child_process(child_process)

                child_returncode = poll_child_returncode(child_process)
                if child_returncode is not None:
                    return confirmed_exit_result(child_returncode), alert_next

                time.sleep(CHILD_WAIT_TIMEOUT_SECONDS)
                continue
            else:
                return confirmed_exit_result(child_returncode), alert_next

            signal_number = shutdown_request["signal"]
            if signal_number is not None and requested_stop_result is None:
                requested_stop_result = ExecutionResult(-15, f"{signal.Signals(signal_number).name} received")

            abort_requested, alert_next = consume_abort_running_with_retry(
                dbname,
                schedule_id,
                server_id,
                interval_mask,
                schedule_log_id,
                requested_stop_result.returncode if requested_stop_result is not None else None,
                alert_next,
            )
            if abort_requested and requested_stop_result is None:
                requested_stop_result = ExecutionResult(-15, "Cicada abort_running")
        except (KeyboardInterrupt, SystemExit) as error:
            if requested_stop_result is None:
                requested_stop_result = execution_result_from_exception(error)
        except Exception as error:
            if supervision_error_detail is None:
                supervision_error_detail = execution_result_from_exception(error).error_detail
            time.sleep(CHILD_WAIT_TIMEOUT_SECONDS)


def run_child_process(
    full_command,
    shutdown_request,
    dbname,
    schedule_id,
    server_id,
    interval_mask,
    schedule_log_id,
    alert_next,
):
    """Launch one child and retain supervision until its exit is confirmed."""
    child_process = subprocess.Popen(full_command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    captured_stderr = bytearray()
    stderr_reader = threading.Thread(
        target=capture_stream_tail,
        args=(child_process.stderr, captured_stderr),
        daemon=True,
    )
    stderr_reader.start()

    execution_result, alert_next = supervise_child_process(
        child_process,
        shutdown_request,
        dbname,
        schedule_id,
        server_id,
        interval_mask,
        schedule_log_id,
        alert_next,
    )
    # A descendant may inherit stderr after the supervised child exits, so do not wait indefinitely for pipe EOF.
    stderr_reader.join(timeout=CHILD_STDERR_DRAIN_TIMEOUT_SECONDS)

    if execution_result.returncode != 0 and execution_result.error_detail is None:
        execution_result = ExecutionResult(execution_result.returncode, stderr_error_detail(captured_stderr))

    return execution_result, alert_next


def finalize_schedule_with_retry(
    dbname,
    schedule_id,
    server_id,
    interval_mask,
    schedule_log_id,
    execution_result,
    alert_next,
):
    """Atomically finalize schedule state, retrying while the database is unavailable."""
    returncode, error_detail = execution_result

    while True:
        try:
            with postgres.db_cicada_cursor(dbname) as (_, db_cur):
                db_cur.execute("BEGIN")
                try:
                    unset_is_running(db_cur, schedule_id)
                    finalize_schedule_log(db_cur, schedule_log_id, returncode, error_detail)
                except Exception:
                    db_cur.execute("ROLLBACK")
                    raise
                db_cur.execute("COMMIT")
            return
        except (KeyboardInterrupt, SystemExit):
            continue
        except Exception as error:
            alert_next = handle_db_unavailable(
                schedule_id,
                server_id,
                interval_mask,
                schedule_log_id,
                returncode,
                "finalize schedule",
                error,
                alert_next,
            )


@utils.named_exception_handler("exec_schedule")
def main(schedule_id, dbname=None):
    """Execute a using schedule_id."""
    shutdown_request = {"signal": None}

    def request_shutdown(signum, _frame):
        shutdown_request["signal"] = signum

    execution_result = None
    schedule_started = False
    schedule_log_id = None
    db_conn_alert_next = next_db_alert_time()
    previous_signal_handlers = {}

    try:
        for signal_number in (signal.SIGTERM, signal.SIGQUIT):
            previous_signal_handlers[signal_number] = signal.signal(signal_number, request_shutdown)

        try:
            with postgres.db_cicada_cursor(dbname) as (_, db_cur):
                server_id = scheduler.get_server_id(db_cur)
                obj_schedule_details = scheduler.get_schedule_executable(db_cur, schedule_id)
                row = obj_schedule_details.fetchone()
                command = str(row[0])
                parameters = str(row[1])
                interval_mask = str(row[2])

                full_command = scheduler.get_full_command(command, parameters)
                human_full_command = str(command + " " + parameters)

                if get_is_running(db_cur, schedule_id) != 0:
                    return

                schedule_log_id = init_schedule_log(db_cur, server_id, schedule_id, human_full_command)
                schedule_started = True
                reset_adhoc_details(db_cur, schedule_id)
                set_is_running(db_cur, schedule_id)

            signal_number = shutdown_request["signal"]
            if signal_number is not None:
                execution_result = ExecutionResult(-15, f"{signal.Signals(signal_number).name} received")
            else:
                execution_result, db_conn_alert_next = run_child_process(
                    full_command,
                    shutdown_request,
                    dbname,
                    schedule_id,
                    server_id,
                    interval_mask,
                    schedule_log_id,
                    db_conn_alert_next,
                )

                if execution_result.returncode != 0:
                    config = utils.load_config()
                    returncodes_alert = config["slack"].get("returncodes_alert", "*")

                    if returncodes_alert == "*" or execution_result.returncode in returncodes_alert:
                        send_slack_error(
                            schedule_id,
                            server_id,
                            interval_mask,
                            schedule_log_id,
                            execution_result.returncode,
                            None,
                            execution_result.error_detail,
                        )

        except (Exception, KeyboardInterrupt, SystemExit) as error:
            if not schedule_started:
                raise
            if execution_result is None:
                execution_result = execution_result_from_exception(error)
        finally:
            if schedule_started:
                finalize_schedule_with_retry(
                    dbname,
                    schedule_id,
                    server_id,
                    interval_mask,
                    schedule_log_id,
                    execution_result,
                    db_conn_alert_next,
                )
    finally:
        for signal_number, previous_handler in previous_signal_handlers.items():
            signal.signal(signal_number, previous_handler)
