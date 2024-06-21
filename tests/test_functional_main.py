"""
    test_functional_main.py
    Test basic functions like
    * registering servers
    * registering various types of schedules
    * execute schedules
    * terminate schedules
"""

import time
import os
import datetime
import socket
import subprocess

from random import choice
from tempfile import TemporaryDirectory

import pytest
from pytest_mock import mocker  # noqa needed for patching
import psycopg2

from cicada.lib import scheduler
from cicada.commands import register_server
from cicada.commands import upsert_schedule
from cicada.commands import exec_schedule
from cicada.commands import exec_server_schedules

from tests.utils.mocks_for_alert import mocks_for_alert_test


@pytest.fixture(scope="session", autouse=True)
def get_env_vars():
    """get_env_vars"""

    pytest.cicada_home = os.environ.get("CICADA_HOME")

    pytest.db_host = os.environ.get("DB_POSTGRES_HOST")
    pytest.db_port = os.environ.get("DB_POSTGRES_PORT")
    pytest.db_user = os.environ.get("DB_POSTGRES_USER")
    pytest.db_pass = os.environ.get("DB_POSTGRES_PASS")

    pytest.db_test = f"pytest_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"


@pytest.fixture()
def db_setup(get_env_vars):
    """db_setup"""

    # Create the test_db
    pg_conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database="postgres",
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()
    pg_cur.execute(f"CREATE DATABASE {pytest.db_test}")

    # Create test_db structure
    test_conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database=pytest.db_test,
    )
    test_conn.autocommit = True
    test_cur = test_conn.cursor()
    test_cur.execute(open(f"{pytest.cicada_home}/setup/schema.sql", "r", encoding="utf-8").read())
    test_cur.close()
    test_conn.close()


def query_test_db(query):
    """Run and SQL query in a postgres database"""
    rows = []
    conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database=pytest.db_test,
    )
    conn.set_session(readonly=False, autocommit=True)

    cur = conn.cursor()

    cur.execute(query)

    if cur.rowcount > 0 and cur.description:
        rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows


def test_test_db_setup(db_setup):
    """test_test_db_setup"""
    query_result = query_test_db("SELECT 1")

    assert query_result == [(1,)]


def test_register_server():
    """test_register_server"""
    query_test_db(
        "INSERT INTO servers (server_id, hostname, fqdn, ip4_address, is_enabled) VALUES (0, 'localhost', 'localhost', '127.0.0.1', 0)"
    )

    register_server.main(pytest.db_test)

    hostname = socket.gethostname()
    if hostname.find(".") != -1:
        hostname = hostname[: hostname.find(".")]
    fqdn = socket.getfqdn()
    ip4_address = socket.gethostbyname(fqdn)

    results = query_test_db(f"SELECT hostname, fqdn, ip4_address, is_enabled FROM servers WHERE hostname='{hostname}'")

    assert results[0][0] == hostname and results[0][1] == fqdn and results[0][2] == ip4_address and results[0][3] == 1


def test_insert_async_schedule():
    """test_insert_async_schedule"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "0.5"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, schedule_description, server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        (
            "pytest",
            None,
            1,
            1,
            1,
            0,
            0,
            "* * * * *",
            datetime.datetime(1000, 1, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
            "sleep",
            "0.5",
            None,
            None,
            0,
        ),
    ]


def test_update_schedule():
    """test_update_schedule"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = 1
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "0.1"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        """
        SELECT schedule_id, schedule_description, server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = 'pytest'
        """
    )

    assert query_result == [
        (
            "pytest",
            None,
            1,
            1,
            1,
            1,
            0,
            "* * * * *",
            datetime.datetime(1000, 1, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
            "sleep",
            "0.1",
            None,
            None,
            0,
        ),
    ]


def test_exec_schedule():
    """test_exec_schedule"""
    exec_schedule.main("pytest", pytest.db_test)

    query_result = query_test_db(
        """SELECT count(*) FROM schedule_log WHERE returncode = 0 AND schedule_id = 'pytest'"""
    )[0][0]

    assert query_result >= 1


def test_exec_schedule_send_alert_if_returncode_not_0_and_config_has_setting_for_all(
    mocker,
):  # noqa
    """test sending alert if returncode is not 0, and config setting is *"""

    return_code = choice(list(range(-100, 0)) + list(range(1, 100)))

    mocked_slack = mocks_for_alert_test(return_code, mocker=mocker)
    definitions_contents = (
        """
        slack:
             channel: foo_channel
        """,
        """
        slack:
             channel: foo_channel
             returncodes_alert: *
        """,
    )
    with TemporaryDirectory() as temp_dir:
        for file_content in definitions_contents:
            with open(f"{temp_dir}/definitions.yml", "w", encoding="utf-8") as definitions_file:
                definitions_file.write(file_content)

            mocker.patch("os.path.join", return_value=f"{temp_dir}/definitions.yml")
            exec_schedule.main("FOO_SCHEDULE_ID", "FOO_DB")
            mocked_slack.assert_called_once_with("FOO_SCHEDULE_ID", "FOO_LOG_ID", return_code, None, None)


def test_exec_schedule_send_alert_if_returncode_not_0_and_exists_in_config(
    mocker,
):  # noqa
    """test sending alert if returncode is not 0, and it is included in the config setting"""
    return_code = choice([1, 13, -15])

    mocked_slack = mocks_for_alert_test(return_code, mocker=mocker)
    with TemporaryDirectory() as temp_dir:
        with open(f"{temp_dir}/definitions.yml", "w", encoding="utf-8") as definitions_file:
            definitions_file.write(
                """
            slack:
                 channel: foo_channel
                 returncodes_alert: [1, 13, -15]"""
            )

        mocker.patch("os.path.join", return_value=f"{temp_dir}/definitions.yml")
        exec_schedule.main("FOO_SCHEDULE_ID", "FOO_DB")
        mocked_slack.assert_called_once_with("FOO_SCHEDULE_ID", "FOO_LOG_ID", return_code, None, None)


def test_exec_schedule_not_send_alert_if_returncode_0_but_not_in_config(mocker):  # noqa
    """test sending no alert if returncode is not 0, but it is not included in the config setting"""
    return_code = choice(list(range(-100, -15)) + list(range(-14, 0)) + list(range(1, 13)) + list(range(14, 100)))

    mocked_slack = mocks_for_alert_test(return_code=return_code, mocker=mocker)
    with TemporaryDirectory() as temp_dir:
        with open(f"{temp_dir}/definitions.yml", "w", encoding="utf-8") as definitions_file:
            definitions_file.write(
                """
                slack:
                     channel: foo_channel
                     returncodes_alert: [13, -15]"""
            )

        mocker.patch("os.path.join", return_value=f"{temp_dir}/definitions.yml")
        exec_schedule.main("FOO_SCHEDULE_ID", "FOO_DB")
        assert mocked_slack.call_count == 0


def test_exec_schedule_not_send_alert_if_returncode_0(mocker):  # noqa
    """test sending no alert if returncode is 0"""
    mocked_slack = mocks_for_alert_test(return_code=0, mocker=mocker)
    exec_schedule.main("FOO_SCHEDULE_ID", "FOO_DB")
    assert mocked_slack.call_count == 0


def test_insert_adhoc_schedule():
    """test_insert_adhoc_schedule"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest_adhoc"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = 1
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "0.5"
    schedule_details["adhoc_parameters"] = "0.1"
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, schedule_description, server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        (
            "pytest_adhoc",
            None,
            1,
            1,
            1,
            0,
            1,
            "* * * * *",
            datetime.datetime(1000, 1, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
            "sleep",
            "0.5",
            "0.1",
            None,
            0,
        ),
    ]


def test_exec_adhoc_schedule():
    """test_exec_adhoc_schedule"""
    exec_schedule.main("pytest_adhoc", pytest.db_test)

    query_result = query_test_db(
        """SELECT count(*) FROM schedule_log WHERE returncode = 0 AND schedule_id = 'pytest_adhoc'"""
    )[0][0]

    assert query_result >= 1


def test_insert_escaped_schedule():
    """test_insert_escaped_schedule"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest_adhoc"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = 1
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "echo"
    schedule_details["parameters"] = "'me'"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, schedule_description, server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        (
            "pytest_adhoc",
            None,
            1,
            1,
            1,
            0,
            1,
            "* * * * *",
            datetime.datetime(1000, 1, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
            "echo",
            "'me'",
            None,
            None,
            0,
        ),
    ]


def test_exec_escaped_schedule():
    """test_exec_escaped_schedule"""
    exec_schedule.main("pytest_adhoc", pytest.db_test)

    query_result = query_test_db(
        """SELECT count(*) FROM schedule_log WHERE returncode = 0 AND schedule_id = 'pytest_adhoc'"""
    )[0][0]

    assert query_result >= 1


def test_insert_sync_schedule_1():
    """test_insert_sync_schedule"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest1"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = 1
    schedule_details["is_async"] = 0
    schedule_details["is_enabled"] = 1
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "0.1"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        """
        SELECT schedule_id, schedule_description, server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = 'pytest1'
        """
    )

    assert query_result == [
        (
            "pytest1",
            None,
            1,
            1,
            0,
            1,
            0,
            "* * * * *",
            datetime.datetime(1000, 1, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
            "sleep",
            "0.1",
            None,
            None,
            0,
        ),
    ]


def test_insert_sync_schedule_2():
    """test_insert_sync_schedule_2"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest2"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = 1
    schedule_details["is_async"] = 0
    schedule_details["is_enabled"] = 1
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "0.1"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        """
        SELECT schedule_id, schedule_description, server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = 'pytest2'
        """
    )

    assert query_result == [
        (
            "pytest2",
            None,
            1,
            1,
            0,
            1,
            0,
            "* * * * *",
            datetime.datetime(1000, 1, 1, 0, 0),
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000),
            "sleep",
            "0.1",
            None,
            None,
            0,
        ),
    ]


def test_exec_server_schedules():
    """test_exec_server_schedules"""

    exec_server_schedules.main(pytest.db_test)

    pytest1_end_time = query_test_db(
        """SELECT end_time FROM schedule_log WHERE returncode = 0 AND schedule_id='pytest1'"""
    )[0][0]
    pytest2_start_time = query_test_db(
        """SELECT start_time FROM schedule_log WHERE returncode = 0 AND schedule_id='pytest2'"""
    )[0][0]

    assert pytest2_start_time > pytest1_end_time


def test_insert_abort_running_1():
    """test_insert_abort_running_1"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest_abort_running_1"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = 1
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "600"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, exec_command, parameters, is_running, abort_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        ("pytest_abort_running_1", "sleep", "600", 0, 1),
    ]


def test_exec_abort_running_1():
    """test_exec_abort_running_1"""
    exec_schedule.main("pytest_abort_running_1", pytest.db_test)

    query_result = query_test_db(
        """SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_abort_running_1'"""
    )

    assert query_result == [("pytest_abort_running_1", -15, "Cicada abort_running")]


def test_insert_abort_running_2():
    """test_insert_abort_running_2"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest_abort_running_2"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = "600"
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, exec_command, parameters, is_running, abort_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        ("pytest_abort_running_2", "sleep", "600", 0, 0),
    ]


# @pytest.mark.skip(reason="not ready yet")
def test_exec_abort_running_2():
    """test_exec_abort_running_2"""
    # exec_schedule.main('pytest_abort_running_2', pytest.db_test)
    full_command = scheduler.generate_exec_schedule_command("pytest_abort_running_2", pytest.db_test)
    subprocess.Popen(full_command)
    time.sleep(1)
    query_test_db("UPDATE schedules SET abort_running=1 WHERE schedule_id='pytest_abort_running_2'")
    time.sleep(10)
    query_result = query_test_db(
        """SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_abort_running_2'"""
    )

    assert query_result == [("pytest_abort_running_2", -15, "Cicada abort_running")]


def test_insert_faulty_schedule_1():
    """Insert schedule with incorrect exec_command"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest_faulty_1"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "fake.exe"
    schedule_details["parameters"] = None
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, exec_command, parameters, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        ("pytest_faulty_1", "fake.exe", None, 0),
    ]


def test_exec_faulty_schedule_1():
    """Run schedule with incorrect exec_command"""
    exec_schedule.main("pytest_faulty_1", pytest.db_test)

    query_result = query_test_db(
        """SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_faulty_1'"""
    )

    print(query_result)

    assert query_result == [("pytest_faulty_1", 2, "No such file or directory")]


def test_insert_faulty_schedule_2():
    """Insert schedule with missing parameter"""

    schedule_details = {}
    schedule_details["schedule_id"] = "pytest_faulty_2"
    schedule_details["schedule_description"] = None
    schedule_details["server_id"] = None
    schedule_details["schedule_order"] = None
    schedule_details["is_async"] = None
    schedule_details["is_enabled"] = None
    schedule_details["adhoc_execute"] = None
    schedule_details["is_running"] = None
    schedule_details["abort_running"] = None
    schedule_details["interval_mask"] = "* * * * *"
    schedule_details["first_run_date"] = None
    schedule_details["last_run_date"] = None
    schedule_details["exec_command"] = "sleep"
    schedule_details["parameters"] = None
    schedule_details["adhoc_parameters"] = None
    schedule_details["schedule_group_id"] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(
        f"""
        SELECT schedule_id, exec_command, parameters, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """
    )

    assert query_result == [
        ("pytest_faulty_2", "sleep", None, 0),
    ]


def test_exec_faulty_schedule_2():
    """Run schedule with missing parameter"""
    exec_schedule.main("pytest_faulty_2", pytest.db_test)

    query_result = query_test_db(
        """SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_faulty_2'"""
    )

    assert query_result == [("pytest_faulty_2", 1, None)]


def test_db_teardown():
    """test_db_teardown"""
    pg_conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database="postgres",
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    pg_cur.execute(f"DROP DATABASE IF EXISTS {pytest.db_test}")
    pg_cur.close()
    pg_conn.close()
