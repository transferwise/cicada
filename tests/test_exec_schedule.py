"""Focused tests for schedule process supervision."""

import datetime
import signal
import subprocess
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
):
    """Run one mocked schedule and return its lifecycle collaborators."""
    db_cursor = MagicMock()
    db_connection = MagicMock()
    db_context = MagicMock()
    db_context.__enter__.return_value = (db_connection, db_cursor)
    db_context.__exit__.return_value = False

    schedule_executable = MagicMock()
    schedule_executable.fetchone.return_value = ("example-command", "example-parameter")

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
    mocker.patch.object(exec_schedule.time, "sleep")
    mocker.patch.object(exec_schedule.utils, "load_config", return_value=config)

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
    child_process.wait.side_effect = [ValueError("unexpected wait failure"), _wait_timeout(), 143]

    collaborators = _run_schedule(
        mocker,
        child_process,
        {"slack": {"returncodes_alert": []}},
        [False],
    )

    child_process.terminate.assert_called_once()
    assert child_process.wait.call_count == 3
    collaborators["unset_is_running"].assert_called_once()
    collaborators["finalize_schedule_log"].assert_called_once_with(
        mocker.ANY,
        "schedule-log-id",
        999,
        "Crazy Unknown Error",
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


def test_db_unavailable_alert_is_rate_limited_and_consistent(mocker):
    """Shared outage handling emits one consistently worded alert when due."""
    send_slack_error = mocker.patch.object(exec_schedule, "send_slack_error")
    sleep = mocker.patch.object(exec_schedule.time, "sleep")
    alert_next = datetime.datetime.utcnow() - datetime.timedelta(seconds=1)

    next_alert = exec_schedule.handle_db_unavailable(
        "example-schedule",
        "schedule-log-id",
        -15,
        "consume abort_running",
        RuntimeError("database unavailable"),
        alert_next,
    )

    send_slack_error.assert_called_once_with(
        "example-schedule",
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
        "schedule-log-id",
        -15,
        "Cicada abort_running",
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
