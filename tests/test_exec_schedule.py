"""Focused tests for schedule process supervision."""

import datetime
import signal
import socket
import subprocess
import sys
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from cicada.commands import exec_schedule
from cicada.lib import postgres


def _wait_timeout():
    return subprocess.TimeoutExpired("example-command", exec_schedule.CHILD_WAIT_TIMEOUT_SECONDS)


def _run_schedule(
    mocker,
    child_process,
    config,
    abort_requests,
    signal_handlers=None,
    events=None,
    signal_during_set=False,
    child_stderr=b"",
):
    """Run one mocked schedule and return its lifecycle collaborators."""
    db_cursor = MagicMock()
    db_connection = MagicMock()
    db_context = MagicMock()
    db_context.__enter__.return_value = (db_connection, db_cursor)
    db_context.__exit__.return_value = False

    schedule_executable = MagicMock()
    schedule_executable.fetchone.return_value = ("example-command", "example-parameter", "*/5 * * * *")

    mocker.patch.object(exec_schedule.postgres, "db_cicada_cursor", return_value=db_context)
    mocker.patch.object(exec_schedule.scheduler, "get_server_id", return_value=1)
    mocker.patch.object(exec_schedule.scheduler, "get_schedule_executable", return_value=schedule_executable)
    mocker.patch.object(exec_schedule.scheduler, "get_full_command", return_value=["example-command"])
    mocker.patch.object(exec_schedule, "get_is_running", return_value=0)
    mocker.patch.object(exec_schedule, "init_schedule_log", return_value="schedule-log-id")
    mocker.patch.object(exec_schedule, "reset_adhoc_details")
    set_is_running = mocker.patch.object(exec_schedule, "set_is_running")
    consume_abort_running = mocker.patch.object(
        exec_schedule,
        "consume_abort_running",
        side_effect=abort_requests,
    )
    unset_is_running = mocker.patch.object(exec_schedule, "unset_is_running")
    finalize_schedule_log = mocker.patch.object(exec_schedule, "finalize_schedule_log")
    if events is not None:
        unset_is_running.side_effect = lambda *_: events.append("unset_is_running")
        finalize_schedule_log.side_effect = lambda *_: events.append("finalize_schedule_log")
    send_slack_error = mocker.patch.object(exec_schedule, "send_slack_error")
    popen = mocker.patch.object(exec_schedule.subprocess, "Popen", return_value=child_process)
    signal_handlers = signal_handlers if signal_handlers is not None else {}
    previous_signal_handlers = {
        signal.SIGTERM: MagicMock(name="previous_sigterm_handler"),
        signal.SIGQUIT: MagicMock(name="previous_sigquit_handler"),
    }

    def install_signal_handler(signal_number, handler):
        previous_handler = signal_handlers.get(signal_number, previous_signal_handlers[signal_number])
        signal_handlers[signal_number] = handler
        return previous_handler

    signal_signal = mocker.patch.object(
        exec_schedule.signal,
        "signal",
        side_effect=install_signal_handler,
    )
    if signal_during_set:
        set_is_running.side_effect = lambda *_: signal_handlers[signal.SIGTERM](signal.SIGTERM, None)
    sleep = mocker.patch.object(exec_schedule.time, "sleep")
    if isinstance(config, Exception):
        load_config = mocker.patch.object(exec_schedule.utils, "load_config", side_effect=config)
    else:
        load_config = mocker.patch.object(exec_schedule.utils, "load_config", return_value=config)
    child_process.stderr = BytesIO(child_stderr)

    exec_schedule.main("example-schedule")

    return {
        "consume_abort_running": consume_abort_running,
        "unset_is_running": unset_is_running,
        "finalize_schedule_log": finalize_schedule_log,
        "send_slack_error": send_slack_error,
        "signal_handlers": signal_handlers,
        "previous_signal_handlers": previous_signal_handlers,
        "signal_signal": signal_signal,
        "popen": popen,
        "sleep": sleep,
        "load_config": load_config,
    }


def test_abort_waits_for_process_exit_before_clearing_running_state(mocker):
    """A confirmed child exit precedes clearing the schedule's running state."""
    events = []
    child_process = MagicMock()
    child_process.terminate.side_effect = lambda: events.append("terminate")
    wait_results = iter([_wait_timeout(), _wait_timeout(), 143])

    def wait_for_child(timeout):
        wait_result = next(wait_results)
        if isinstance(wait_result, Exception):
            raise wait_result
        events.append("exit")
        return wait_result

    child_process.wait.side_effect = wait_for_child

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [True, False],
        events=events,
    )

    assert events == ["terminate", "exit", "unset_is_running", "finalize_schedule_log"]
    collaborators["consume_abort_running"].assert_has_calls(
        [mocker.call(None, "example-schedule"), mocker.call(None, "example-schedule")]
    )
    collaborators["unset_is_running"].assert_called_once_with(mocker.ANY, "example-schedule")
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        -15,
        "Cicada abort_running",
    )
    collaborators["send_slack_error"].assert_not_called()


def test_abort_consumes_repeated_requests_while_waiting_for_exit(mocker):
    """Repeated abort requests are consumed while a child delays its SIGTERM exit."""
    child_process = MagicMock()
    child_process.wait.side_effect = [_wait_timeout(), _wait_timeout(), _wait_timeout(), 143]

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [True, True, False],
    )

    assert collaborators["consume_abort_running"].call_count == 3
    child_process.terminate.assert_called_once()
    assert child_process.wait.call_count == 4
    collaborators["unset_is_running"].assert_called_once()


def test_natural_child_exit_does_not_request_termination(mocker):
    """A natural child exit is returned directly without polling the abort flag."""
    child_process = MagicMock()
    child_process.wait.return_value = 0

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [],
    )

    child_process.wait.assert_called_once_with(timeout=exec_schedule.CHILD_WAIT_TIMEOUT_SECONDS)
    child_process.terminate.assert_not_called()
    collaborators["consume_abort_running"].assert_not_called()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        0,
        None,
    )


def test_failed_child_stderr_is_saved_and_sent_to_slack(mocker):
    """A Docker launch error is retained in schedule_log and its Slack alert."""
    child_process = MagicMock()
    child_process.wait.return_value = 125
    docker_error = (
        b'docker: Error response from daemon: Conflict. The container name "pipelinewise-tap-one" is in use.\n'
    )

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": "*"}},
        [],
        child_stderr=docker_error,
    )

    error_detail = docker_error.decode().strip()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        125,
        error_detail,
    )
    collaborators["send_slack_error"].assert_called_once_with(
        "example-schedule",
        1,
        "*/5 * * * *",
        "schedule-log-id",
        125,
        None,
        error_detail,
    )


def test_failed_child_stderr_is_bounded(mocker):
    """Only the final database-sized portion of verbose child stderr is retained."""
    child_process = MagicMock()
    child_process.wait.return_value = 125
    final_error = b"final Docker error"

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [],
        child_stderr=(b"x" * (exec_schedule.CHILD_STDERR_CAPTURE_MAX_BYTES + 1000)) + final_error,
    )

    error_detail = collaborators["finalize_schedule_log"].call_args.args[3]
    assert len(error_detail) == exec_schedule.ERROR_DETAIL_MAX_LENGTH
    assert error_detail.endswith(final_error.decode())


def test_large_child_stderr_is_drained_without_blocking(mocker):
    """A child can exceed the OS pipe buffer and still exit cleanly."""
    mocker.patch.object(exec_schedule, "consume_abort_running", return_value=False)
    final_error = "final Docker error"

    execution_result, _ = exec_schedule.run_child_process(
        [
            sys.executable,
            "-c",
            f"import sys; sys.stderr.write('x' * 100000 + {final_error!r}); raise SystemExit(125)",
        ],
        {"signal": None},
        None,
        "example-schedule",
        1,
        "*/5 * * * *",
        "schedule-log-id",
        datetime.datetime.utcnow(),
    )

    assert execution_result.returncode == 125
    assert len(execution_result.error_detail) == exec_schedule.ERROR_DETAIL_MAX_LENGTH
    assert execution_result.error_detail.endswith(final_error)


def test_failed_termination_is_retried_while_supervision_continues(mocker):
    """A failed SIGTERM delivery cannot be mistaken for successful termination."""
    child_process = MagicMock()
    child_process.wait.side_effect = [_wait_timeout(), _wait_timeout(), 143]
    child_process.terminate.side_effect = [OSError("signal unavailable"), None]

    _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [True, False],
    )

    assert child_process.terminate.call_count == 2


def test_signal_handlers_cover_state_transition_and_are_restored(mocker):
    """Shutdown handling is active before is_running changes and does not leak after main returns."""
    child_process = MagicMock()

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [],
        signal_during_set=True,
    )

    collaborators["popen"].assert_not_called()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        -15,
        "SIGTERM received",
    )
    assert collaborators["signal_handlers"] == collaborators["previous_signal_handlers"]


def test_clearing_running_state_also_clears_outstanding_abort_request():
    """The final state transition does not leave a flag for the next run."""
    db_cursor = MagicMock()

    exec_schedule.unset_is_running(db_cursor, "example-schedule")

    sqlquery, parameters = db_cursor.execute.call_args.args
    assert "is_running = 0" in sqlquery
    assert "abort_running = 0" in sqlquery
    assert parameters == ("example-schedule",)


def test_wait_error_keeps_supervising_until_child_exit(mocker):
    """A transient wait error cannot finalize a schedule while its child may be alive."""
    child_process = MagicMock()
    child_process.poll.return_value = None
    child_process.wait.side_effect = [
        _wait_timeout(),
        OSError(-15, "wait interrupted"),
        _wait_timeout(),
        143,
    ]

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [True, False, False],
    )

    child_process.terminate.assert_called_once()
    assert child_process.wait.call_count == 4
    collaborators["unset_is_running"].assert_called_once()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        -15,
        "Cicada abort_running",
    )


def test_sigterm_terminates_and_supervises_child_until_exit(mocker):
    """A handled SIGTERM cannot orphan the child or leave is_running stuck."""
    child_process = MagicMock()
    wait_results = iter([_wait_timeout(), _wait_timeout(), 143])
    signal_handlers = {}

    def wait_for_child(timeout):
        result = next(wait_results)
        if child_process.wait.call_count == 1:
            signal_handlers[signal.SIGTERM](signal.SIGTERM, None)
        if isinstance(result, Exception):
            raise result
        return result

    child_process.wait.side_effect = wait_for_child
    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [False, False],
        signal_handlers,
    )

    child_process.terminate.assert_called_once()
    assert child_process.wait.call_count == 3
    collaborators["unset_is_running"].assert_called_once()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        -15,
        "SIGTERM received",
    )


def test_unexpected_supervision_error_terminates_and_waits_for_child(mocker):
    """The final fallback keeps ownership of a child after an unexpected error."""
    child_process = MagicMock()
    child_process.poll.return_value = None
    child_process.wait.side_effect = [ValueError("unexpected wait failure"), _wait_timeout(), 143]

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [False],
    )

    child_process.terminate.assert_called_once()
    assert child_process.wait.call_count == 3
    collaborators["sleep"].assert_called_once_with(exec_schedule.CHILD_WAIT_TIMEOUT_SECONDS)
    collaborators["unset_is_running"].assert_called_once()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        143,
        "unexpected wait failure",
    )


def test_repeated_unexpected_wait_errors_back_off_and_use_poll_to_confirm_exit(mocker):
    """Repeated wait failures use a safe fallback without clearing a live schedule."""
    child_process = MagicMock()
    child_process.wait.side_effect = ValueError("persistent wait failure")
    child_process.poll.side_effect = [None, None, 143]

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [],
    )

    assert child_process.wait.call_count == 3
    child_process.terminate.assert_called_once()
    assert child_process.poll.call_count == 3
    assert collaborators["sleep"].call_count == 2
    collaborators["unset_is_running"].assert_called_once()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        143,
        "persistent wait failure",
    )


def test_alternating_wait_errors_poll_until_child_exit_is_confirmed(mocker):
    """Different wait failures cannot prevent the poll fallback from observing exit."""
    child_process = MagicMock()
    child_process.wait.side_effect = [
        ValueError("unexpected wait failure"),
        _wait_timeout(),
        ValueError("unexpected wait failure"),
    ]
    child_process.poll.side_effect = [None, 137]

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [False],
    )

    assert child_process.wait.call_count == 3
    assert child_process.poll.call_count == 2
    assert collaborators["sleep"].call_count == 1
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        137,
        "unexpected wait failure",
    )


def test_abort_result_is_preserved_when_poll_confirms_exit_after_wait_error(mocker):
    """A fallback process check cannot replace an explicit abort result."""
    child_process = MagicMock()
    child_process.wait.side_effect = [
        _wait_timeout(),
        ValueError("unexpected wait failure"),
    ]
    child_process.poll.return_value = 143

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [True],
    )

    child_process.terminate.assert_called_once()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        -15,
        "Cicada abort_running",
    )


def test_alert_configuration_error_does_not_replace_child_result(mocker):
    """A post-exit alert error cannot replace the process's confirmed result."""
    child_process = MagicMock()
    child_process.wait.return_value = 137

    collaborators = _run_schedule(
        mocker,
        child_process,
        RuntimeError("alert configuration unavailable"),
        [],
    )

    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        137,
        None,
    )
    collaborators["load_config"].assert_called_once_with()


def test_oserror_without_errno_uses_unknown_return_code():
    """An OSError without errno still produces a valid schedule return code."""
    result = exec_schedule.execution_result_from_exception(socket.timeout("worker timed out"))

    assert result == exec_schedule.ExecutionResult(exec_schedule.UNKNOWN_RETURN_CODE, "worker timed out")


def test_finalize_schedule_log_parameterizes_unknown_return_code_and_error_detail():
    """Final log values cannot change the SQL statement or make it invalid."""
    db_cursor = MagicMock()

    exec_schedule.finalize_schedule_log(
        db_cursor,
        "schedule-log-id",
        None,
        "worker's connection timed out",
    )

    sqlquery, parameters = db_cursor.execute.call_args.args
    assert "returncode = %s" in sqlquery
    assert "error_detail = %s" in sqlquery
    assert "schedule_log_id = %s" in sqlquery
    assert "worker's connection timed out" not in sqlquery
    assert parameters == (
        exec_schedule.UNKNOWN_RETURN_CODE,
        "worker's connection timed out",
        "schedule-log-id",
    )


def test_finalize_schedule_log_truncates_error_detail_to_database_limit():
    """An oversized process error cannot prevent schedule finalization."""
    db_cursor = MagicMock()

    exec_schedule.finalize_schedule_log(
        db_cursor,
        "schedule-log-id",
        exec_schedule.UNKNOWN_RETURN_CODE,
        "x" * (exec_schedule.ERROR_DETAIL_MAX_LENGTH + 1),
    )

    _, parameters = db_cursor.execute.call_args.args
    assert len(parameters[1]) == 255
    assert parameters == (
        exec_schedule.UNKNOWN_RETURN_CODE,
        "x" * 255,
        "schedule-log-id",
    )


@pytest.mark.parametrize(
    ("returned_row", "expected"),
    [(("example-schedule",), True), (None, False)],
)
def test_consume_abort_running_is_atomic_and_parameterized(mocker, returned_row, expected):
    """Abort consumption uses one conditional UPDATE and reports whether it changed a row."""
    db_cursor = MagicMock()
    db_cursor.fetchone.return_value = returned_row
    db_context = MagicMock()
    db_context.__enter__.return_value = (MagicMock(), db_cursor)
    db_context.__exit__.return_value = False
    mocker.patch.object(exec_schedule.postgres, "db_cicada_cursor", return_value=db_context)

    assert exec_schedule.consume_abort_running("example-db", "example-schedule") is expected

    sqlquery, parameters = db_cursor.execute.call_args.args
    assert "UPDATE schedules" in sqlquery
    assert "abort_running = 0" in sqlquery
    assert "abort_running = 1" in sqlquery
    assert "RETURNING schedule_id" in sqlquery
    assert parameters == ("example-schedule",)
    db_cursor.execute.assert_called_once()


def test_db_cursor_creation_failure_closes_connection(mocker):
    """A cursor construction failure cannot leak its opened connection."""
    db_connection = MagicMock()
    db_connection.cursor.side_effect = RuntimeError("cursor unavailable")
    mocker.patch.object(postgres, "db_cicada", return_value=db_connection)

    with pytest.raises(RuntimeError, match="cursor unavailable"):
        with postgres.db_cicada_cursor("example-db"):
            pass

    db_connection.close.assert_called_once()


def test_slack_error_includes_server_and_interval(mocker):
    """Execution alerts identify the server and schedule interval."""
    send_slack_message = mocker.patch.object(exec_schedule.utils, "send_slack_message")

    exec_schedule.send_slack_error(
        "example-schedule",
        7,
        "*/5 * * * *",
        "schedule-log-id",
        125,
        None,
        None,
    )

    message = send_slack_message.call_args.args[1]
    assert "server_id       : 7" in message
    assert "interval_mask   : */5 * * * *" in message
    assert message.index("server utc time") < message.index("schedule_log_id")
    assert message.index("schedule_log_id") < message.index("server_id")
    assert message.index("server_id") < message.index("interval_mask")


def test_db_unavailable_alert_is_rate_limited_and_consistent(mocker):
    """Shared outage handling emits one consistently worded alert when due."""
    send_slack_error = mocker.patch.object(exec_schedule, "send_slack_error")
    sleep = mocker.patch.object(exec_schedule.time, "sleep")
    alert_next = datetime.datetime.utcnow() - datetime.timedelta(seconds=1)

    next_alert = exec_schedule.handle_db_unavailable(
        "example-schedule",
        1,
        "*/5 * * * *",
        "schedule-log-id",
        -15,
        "consume abort_running",
        RuntimeError("database unavailable"),
        alert_next,
    )

    send_slack_error.assert_called_once_with(
        "example-schedule",
        1,
        "*/5 * * * *",
        "schedule-log-id",
        -15,
        "Cicada db unavailable - consume abort_running - 15 minutes",
        mocker.ANY,
    )
    sleep.assert_called_once_with(exec_schedule.DB_RETRY_DELAY_SECONDS)
    assert next_alert > alert_next

    send_slack_error.reset_mock()
    future_alert = datetime.datetime.utcnow() + datetime.timedelta(minutes=1)

    unchanged_alert = exec_schedule.handle_db_unavailable(
        "example-schedule",
        1,
        "*/5 * * * *",
        "schedule-log-id",
        -15,
        "finalize schedule",
        RuntimeError("database unavailable"),
        future_alert,
    )

    send_slack_error.assert_not_called()
    assert unchanged_alert == future_alert


def test_finalization_rolls_back_and_retries_as_one_transaction(mocker):
    """State and log finalization cannot be partially committed."""
    events = []
    contexts = []

    for attempt in (1, 2):
        db_cursor = MagicMock()
        db_cursor.execute.side_effect = lambda sqlquery, _attempt=attempt: events.append(
            f"{_attempt}:{sqlquery.strip()}"
        )
        db_context = MagicMock()
        db_context.__enter__.return_value = (MagicMock(), db_cursor)
        db_context.__exit__.return_value = False
        contexts.append(db_context)

    mocker.patch.object(exec_schedule.postgres, "db_cicada_cursor", side_effect=contexts)
    mocker.patch.object(exec_schedule, "unset_is_running", side_effect=lambda *_: events.append("unset"))
    finalize_attempt = 0

    def finalize_schedule(*_args):
        nonlocal finalize_attempt
        finalize_attempt += 1
        if finalize_attempt == 1:
            raise RuntimeError("write failed")
        events.append("finalize")

    mocker.patch.object(exec_schedule, "finalize_schedule_log", side_effect=finalize_schedule)
    mocker.patch.object(
        exec_schedule,
        "handle_db_unavailable",
        side_effect=lambda *args: events.append("retry") or args[-1],
    )

    exec_schedule.finalize_schedule_with_retry(
        "example-db",
        "example-schedule",
        1,
        "*/5 * * * *",
        "schedule-log-id",
        exec_schedule.ExecutionResult(-15, "Cicada abort_running"),
        datetime.datetime.utcnow(),
    )

    assert events == [
        "1:BEGIN",
        "unset",
        "1:ROLLBACK",
        "retry",
        "2:BEGIN",
        "unset",
        "finalize",
        "2:COMMIT",
    ]
