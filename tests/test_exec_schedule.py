"""Focused tests for schedule process supervision."""

from unittest.mock import MagicMock

from cicada.commands import exec_schedule


def test_abort_waits_for_child_exit_before_clearing_running_state(mocker):
    """Keep a schedule running until its aborted child process exits."""
    events = []
    db_connection = MagicMock()
    db_cursor = db_connection.cursor.return_value
    schedule_executable = MagicMock()
    schedule_executable.fetchone.return_value = ("example-command", "example-parameter")
    child_process = MagicMock()
    child_returncodes = iter([None, None, 143])

    def poll_child_process():
        returncode = next(child_returncodes)
        if returncode is not None:
            events.append("child_exit")
        return returncode

    child_process.poll.side_effect = poll_child_process
    child_process.terminate.side_effect = lambda: events.append("terminate")

    mocker.patch.object(exec_schedule.postgres, "db_cicada", return_value=db_connection)
    mocker.patch.object(exec_schedule.scheduler, "get_server_id", return_value=1)
    mocker.patch.object(exec_schedule.scheduler, "get_schedule_executable", return_value=schedule_executable)
    mocker.patch.object(exec_schedule.scheduler, "get_full_command", return_value=["example-command"])
    mocker.patch.object(exec_schedule, "get_is_running", return_value=0)
    mocker.patch.object(exec_schedule, "init_schedule_log", return_value="schedule-log-id")
    mocker.patch.object(exec_schedule, "reset_adhoc_details")
    mocker.patch.object(exec_schedule, "set_is_running")
    mocker.patch.object(exec_schedule, "get_abort_running", side_effect=[True, True])
    unset_abort_running = mocker.patch.object(exec_schedule, "unset_abort_running")
    unset_is_running = mocker.patch.object(
        exec_schedule,
        "unset_is_running",
        side_effect=lambda *_: events.append("unset_is_running"),
    )
    finalize_schedule_log = mocker.patch.object(
        exec_schedule,
        "finalize_schedule_log",
        side_effect=lambda *_: events.append("finalize_schedule_log"),
    )
    mocker.patch.object(exec_schedule.subprocess, "Popen", return_value=child_process)
    mocker.patch.object(exec_schedule.signal, "signal")
    mocker.patch.object(exec_schedule.time, "sleep")
    mocker.patch.object(exec_schedule.utils, "load_config", return_value={"slack": {"returncodes_alert": []}})

    exec_schedule.main("example-schedule")

    assert events == ["terminate", "child_exit", "unset_is_running", "finalize_schedule_log"]
    child_process.terminate.assert_called_once_with()
    assert unset_abort_running.call_count == 2
    unset_is_running.assert_called_once_with(db_cursor, "example-schedule")
    finalize_schedule_log.assert_called_once_with(
        db_cursor,
        "schedule-log-id",
        -15,
        "Cicada abort_running",
    )
