class MockPopen:
    def __init__(self, return_code, *args, **kwargs):
        self.return_code = return_code

    def wait(self, timeout=None):
        return self.return_code


def mocks_for_alert_test(return_code, mocker):
    mocker.patch("cicada.commands.exec_schedule.subprocess.Popen", return_value=MockPopen(return_code))
    mocker.patch("cicada.commands.exec_schedule.init_schedule_log", return_value="FOO_LOG_ID")
    db_context = mocker.MagicMock()
    db_context.__enter__.return_value = (mocker.MagicMock(), mocker.MagicMock())
    db_context.__exit__.return_value = False
    mocker.patch("cicada.commands.exec_schedule.postgres.db_cicada_cursor", return_value=db_context)
    mocker.patch("cicada.lib.scheduler.get_server_id", return_value=7)
    schedule_executable = mocker.MagicMock()
    schedule_executable.fetchone.return_value = ("example-command", "example-parameter", "*/5 * * * *")
    mocker.patch("cicada.lib.scheduler.get_schedule_executable", return_value=schedule_executable)
    mocker.patch("cicada.lib.scheduler.get_full_command", return_value=["example-command"])
    mocker.patch("cicada.commands.exec_schedule.get_is_running", return_value=0)
    mocked_send_slack = mocker.patch("cicada.commands.exec_schedule.send_slack_error")
    return mocked_send_slack
