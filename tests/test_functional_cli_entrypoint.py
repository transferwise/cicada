"""test_cicada_entrypoint.py"""

import subprocess


def test_cicada():
    """test_cicada"""
    actual = subprocess.run(["cicada"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: cicada [-h] command
cicada: error: the following arguments are required: command
"""
    assert actual == expected


def test_cicada_help():
    """test_cicada_help"""
    actual = subprocess.run(["cicada", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode("utf-8")

    expected = """usage: cicada [-h] command

positional arguments:
  command     register_server , list_server_schedules , exec_server_schedules
              , show_schedule , upsert_schedule , exec_schedule ,
              spread_schedules , archive_schedule_log , ping_slack ,
              list_schedule_ids , delete_schedule , version

optional arguments:
  -h, --help  show this help message and exit
"""
    assert actual == expected


def test_bad_command():
    """test_bad_command"""
    actual = subprocess.run(["cicada", "blah"], check=False, stdout=subprocess.PIPE).stdout.decode("utf-8")

    expected = """blah is not a recognized command

usage: cicada [-h] command

positional arguments:
  command     register_server , list_server_schedules , exec_server_schedules
              , show_schedule , upsert_schedule , exec_schedule ,
              spread_schedules , archive_schedule_log , ping_slack ,
              list_schedule_ids , delete_schedule , version

optional arguments:
  -h, --help  show this help message and exit
"""
    assert actual == expected


def test_show_schedule():
    """test_show_schedule"""
    actual = subprocess.run(["cicada", "show_schedule"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: show_schedule [-h] --schedule_id SCHEDULE_ID
show_schedule: error: the following arguments are required: --schedule_id
"""
    assert actual == expected


def test_show_schedule_help():
    """test_show_schedule_help"""
    actual = subprocess.run(["cicada", "show_schedule", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: show_schedule [-h] --schedule_id SCHEDULE_ID

List a schedule using schedule_id

optional arguments:
  -h, --help            show this help message and exit
  --schedule_id SCHEDULE_ID
                        Id of the schedule
"""
    assert actual == expected


def test_upsert_schedule():
    """test_upsert_schedule"""
    actual = subprocess.run(["cicada", "upsert_schedule"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: upsert_schedule [-h] --schedule_id SCHEDULE_ID
                       [--schedule_description SCHEDULE_DESCRIPTION]
                       [--server_id SERVER_ID]
                       [--schedule_order SCHEDULE_ORDER] [--is_async IS_ASYNC]
                       [--is_enabled IS_ENABLED]
                       [--adhoc_execute ADHOC_EXECUTE]
                       [--abort_running ABORT_RUNNING]
                       [--interval_mask INTERVAL_MASK]
                       [--first_run_date FIRST_RUN_DATE]
                       [--last_run_date LAST_RUN_DATE]
                       [--exec_command EXEC_COMMAND] [--parameters PARAMETERS]
                       [--adhoc_parameters ADHOC_PARAMETERS]
                       [--schedule_group_id SCHEDULE_GROUP_ID]
upsert_schedule: error: the following arguments are required: --schedule_id
"""
    assert actual == expected


def test_upsert_schedule_help():
    """test_upsert_schedule_help"""
    actual = subprocess.run(["cicada", "upsert_schedule", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: upsert_schedule [-h] --schedule_id SCHEDULE_ID
                       [--schedule_description SCHEDULE_DESCRIPTION]
                       [--server_id SERVER_ID]
                       [--schedule_order SCHEDULE_ORDER] [--is_async IS_ASYNC]
                       [--is_enabled IS_ENABLED]
                       [--adhoc_execute ADHOC_EXECUTE]
                       [--abort_running ABORT_RUNNING]
                       [--interval_mask INTERVAL_MASK]
                       [--first_run_date FIRST_RUN_DATE]
                       [--last_run_date LAST_RUN_DATE]
                       [--exec_command EXEC_COMMAND] [--parameters PARAMETERS]
                       [--adhoc_parameters ADHOC_PARAMETERS]
                       [--schedule_group_id SCHEDULE_GROUP_ID]

Upsert a schedule using schedule_id

optional arguments:
  -h, --help            show this help message and exit
  --schedule_id SCHEDULE_ID
                        Id of the schedule
  --schedule_description SCHEDULE_DESCRIPTION
                        Schedule description and comments
  --server_id SERVER_ID
                        Id of the server where the schedule will run
  --schedule_order SCHEDULE_ORDER
                        run order for the schedule. lowest is first. is_async
                        jobs will be execute in parallel
  --is_async IS_ASYNC   0=disabled 1=Enabled | is_async jobs execute in
                        parallel
  --is_enabled IS_ENABLED
                        0=Disabled 1=Enabled
  --adhoc_execute ADHOC_EXECUTE
                        0=Disabled 1=Enabled | Execute at next minute,
                        regardless of other settings
  --abort_running ABORT_RUNNING
                        0=Disabled 1=Enabled | If the job is running, it will
                        be terminated as soon as possible
  --interval_mask INTERVAL_MASK
                        When to execute the command | unix crontab (minute
                        hour dom month dow)
  --first_run_date FIRST_RUN_DATE
                        The schedule will not execute before this datetime
  --last_run_date LAST_RUN_DATE
                        The schedule will not execute after this datetime
  --exec_command EXEC_COMMAND
                        Command to execute
  --parameters PARAMETERS
                        Parameters for exec_command
  --adhoc_parameters ADHOC_PARAMETERS
                        If specified, will override parameters for one run
  --schedule_group_id SCHEDULE_GROUP_ID
                        Optional field to help with schedule grouping
"""
    assert actual == expected


def test_exec_schedule():
    """test_exec_schedule"""
    actual = subprocess.run(["cicada", "exec_schedule"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: exec_schedule [-h] --schedule_id SCHEDULE_ID
exec_schedule: error: the following arguments are required: --schedule_id
"""
    assert actual == expected


def test_exec_schedule_help():
    """test_exec_schedule_help"""
    actual = subprocess.run(["cicada", "exec_schedule", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: exec_schedule [-h] --schedule_id SCHEDULE_ID

Execute a using schedule_id

optional arguments:
  -h, --help            show this help message and exit
  --schedule_id SCHEDULE_ID
                        Id of the schedule
"""
    assert actual == expected


def test_spread_schedules():
    """test_spread_schedules"""
    actual = subprocess.run(["cicada", "spread_schedules"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: spread_schedules [-h] --from_server_ids FROM_SERVER_IDS --to_server_ids
                        TO_SERVER_IDS [--exclude_disabled_servers] [--commit]
                        [--force]
spread_schedules: error: the following arguments are required: --from_server_ids, --to_server_ids
"""

    assert actual == expected


def test_spread_schedules_help():
    """test_spread_schedules_help"""
    actual = subprocess.run(["cicada", "spread_schedules", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: spread_schedules [-h] --from_server_ids FROM_SERVER_IDS --to_server_ids
                        TO_SERVER_IDS [--exclude_disabled_servers] [--commit]
                        [--force]

Spread schedules accross servers

optional arguments:
  -h, --help            show this help message and exit
  --from_server_ids FROM_SERVER_IDS
                        List of source server_ids to collect schedules from
  --to_server_ids TO_SERVER_IDS
                        List of target server_ids to spread schedules to
  --exclude_disabled_servers
                        Exclude disabled servers from target server_ids
  --commit              Commits changes to backend DB, otherwise only print
                        output
  --force               If schedule is moving servers and also currently
                        running, perform abort_running and adhoc_execute |
                        Only available when --commit is specified
"""

    assert actual == expected


def test_archive_schedule_log():
    """test_archive_schedule_log"""
    actual = subprocess.run(["cicada", "archive_schedule_log"], check=False, stderr=subprocess.PIPE).stderr.decode(
        "utf-8"
    )

    expected = """usage: archive_schedule_log [-h] --days_to_keep DAYS_TO_KEEP
archive_schedule_log: error: the following arguments are required: --days_to_keep
"""
    assert actual == expected


def test_archive_schedule_log_help():
    """test_archive_schedule_log_help"""
    actual = subprocess.run(["cicada", "archive_schedule_log", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: archive_schedule_log [-h] --days_to_keep DAYS_TO_KEEP

Archive entries from schedule_log into schedule_log_historical

optional arguments:
  -h, --help            show this help message and exit
  --days_to_keep DAYS_TO_KEEP
                        Amount of days to keep in schedule_log
"""
    assert actual == expected


def test_ping_slack():
    """test_ping_slack"""
    actual = subprocess.run(["cicada", "ping_slack"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: ping_slack [-h] --text TEXT
ping_slack: error: the following arguments are required: --text
"""
    assert actual == expected


def test_ping_slack_help():
    """test_ping_slack_help"""
    actual = subprocess.run(["cicada", "ping_slack", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode("utf-8")

    expected = """usage: ping_slack [-h] --text TEXT

Send a test message to Slack

optional arguments:
  -h, --help   show this help message and exit
  --text TEXT  Text to send to Slack
"""
    assert actual == expected


def test_delete_schedule():
    """test_delete_schedule"""
    actual = subprocess.run(["cicada", "delete_schedule"], check=False, stderr=subprocess.PIPE).stderr.decode("utf-8")

    expected = """usage: delete_schedule [-h] --schedule_id SCHEDULE_ID
delete_schedule: error: the following arguments are required: --schedule_id
"""
    assert actual == expected


def test_delete_schedule_help():
    """test_delete_schedule_help"""
    actual = subprocess.run(["cicada", "delete_schedule", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: delete_schedule [-h] --schedule_id SCHEDULE_ID

Delete a schedule using schedule_id

optional arguments:
  -h, --help            show this help message and exit
  --schedule_id SCHEDULE_ID
                        Id of the schedule
"""
    assert actual == expected


def test_list_schedule_ids():
    """test_list_schedule_ids"""
    actual = subprocess.run(["cicada", "list_schedule_ids", "-h"], check=True, stdout=subprocess.PIPE).stdout.decode(
        "utf-8"
    )

    expected = """usage: list_schedule_ids [-h]

List schedule ids of all schedules

optional arguments:
  -h, --help  show this help message and exit
"""
    assert actual == expected
