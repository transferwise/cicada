class MockPopen:
    def __init__(self, return_code, *args, **kwargs):
        self.return_code = return_code

    def poll(self):
        return self.return_code


def mocks_for_alert_test(return_code, mocker):
    mocker.patch("cicada.commands.exec_schedule.subprocess.Popen", return_value=MockPopen(return_code))
    mocker.patch("cicada.commands.exec_schedule.init_schedule_log", return_value="FOO_LOG_ID")
    mocker.patch("cicada.commands.exec_schedule.postgres")
    mocker.patch("cicada.lib.scheduler.get_server_id")
    mocker.patch("cicada.commands.exec_schedule.get_is_running", return_value=0)
    mocked_send_slack = mocker.patch("cicada.commands.exec_schedule.send_slack_error")
    return mocked_send_slack
