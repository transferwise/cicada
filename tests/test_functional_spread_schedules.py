"""test_main_functionality.py"""

import pytest
import os
import datetime
import psycopg2
from freezegun import freeze_time

from cicada.commands import spread_schedules


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
    with psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database=pytest.db_test,
    ) as conn:
        conn.set_session(readonly=False, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.rowcount > 0 and cur.description:
                rows = cur.fetchall()
    return rows


def test_test_db_setup(db_setup):
    """test_test_db_setup"""
    query_result = query_test_db("SELECT 1")

    assert query_result == [(1,)]


def test_create_dummy_servers():
    """test_create_dummy_servers"""
    query_test_db(
        """
        INSERT INTO servers
            (server_id, hostname, fqdn, ip4_address, is_enabled)
        VALUES
            (1, '1', '1', '192.168.0.1', 1),
            (2, '2', '2', '192.168.0.2', 1),
            (3, '3', '3', '192.168.0.3', 0),
            (4, '4', '4', '192.168.0.4', 1)
        ;
        """
    )

    results = query_test_db(f"SELECT count(*) FROM servers")[0][0]

    assert results == 4


def test_create_dummy_schedules():
    """test_create_dummy_schedules"""
    query_test_db(
        """
        INSERT INTO schedules
            (server_id, schedule_id, interval_mask, exec_command, is_enabled, is_running)
        VALUES
            (1, '1-1', '* * * * *', '1-1', 1, 0),
            (1, '1-2', '* * * * *', '1-2', 1, 1),
            (1, '1-3', '* * * * *', '1-3', 1, 0),
            (1, '1-4', '* * * * *', '1-4', 1, 0),
            (2, '2-1', '* * * * *', '2-1', 1, 0),
            (2, '2-2', '* * * * *', '2-2', 1, 0),
            (2, '2-3', '* * * * *', '2-3', 1, 0),
            (2, '2-4', '* * * * *', '2-4', 1, 0),
            (3, '3-1', '* * * * *', '3-1', 1, 0),
            (3, '3-2', '* * * * *', '3-2', 1, 0),
            (3, '3-3', '* * * * *', '3-3', 1, 0),
            (3, '3-4', '* * * * *', '3-4', 1, 0)
        ;
        """
    )

    results = query_test_db(f"SELECT count(*) FROM schedules WHERE is_enabled=1")[0][0]

    assert results == 12


def test_create_dummy_schedule_logs():
    """test_create_dummy_schedule_logs"""
    query_test_db(
        """
        INSERT INTO schedule_log
            (server_id, schedule_id, full_command, start_time, end_time, returncode)
        VALUES
             (1, '1-1', '1-1', '2022-01-01 01:01:00', '2022-01-01 02:00:00' , 0)
            ,(1, '1-2', '1-2', '2022-01-01 02:01:00', '2022-01-01 03:00:00' , 0)
            ,(1, '1-3', '1-3', '2022-01-01 03:01:00', '2022-01-01 04:00:00' , 0)
            ,(1, '1-4', '1-4', '2022-01-01 04:01:00', '2022-01-01 05:00:00' , 0)

            ,(2, '2-1', '2-1', '2022-01-01 01:01:00', '2022-01-01 03:00:00' , 0)
            ,(2, '2-2', '2-2', '2022-01-01 03:01:00', '2022-01-01 05:00:00' , 0)
            ,(2, '2-3', '2-3', '2022-01-01 05:01:00', '2022-01-01 07:00:00' , 0)
            ,(2, '2-4', '2-4', '2022-01-01 07:01:00', '2022-01-01 09:00:00' , 0)

            ,(3, '3-1', '3-1', '2022-01-01 01:01:00', '2022-01-01 04:00:00' , 0)
            ,(3, '3-2', '3-2', '2022-01-01 04:01:00', '2022-01-01 07:00:00' , 0)
            ,(3, '3-3', '3-3', '2022-01-01 07:01:00', '2022-01-01 10:00:00' , 0)
            ,(3, '3-4', '3-4', '2022-01-01 10:01:00', '2022-01-01 13:00:00' , 0)

            /* Logs for schedules that longer exist in schedules table */
            ,(1, '1-5', '1-5', '2022-01-01 01:01:00', '2022-01-01 02:00:00' , 0)
            ,(2, '1-6', '1-6', '2022-01-01 01:01:00', '2022-01-01 03:00:00' , 0)
            ,(3, '1-7', '1-7', '2022-01-01 01:01:00', '2022-01-01 04:00:00' , 0)
        ;
        """
    )

    results = query_test_db(f"SELECT count(*) FROM schedule_log")[0][0]

    assert results == 15


@freeze_time("2022-01-02 02:00:00")
def test_spread_schedules():
    """test_spread_schedules"""
    spread_details = {}

    spread_details["from_server_ids"] = "1,2,3"
    spread_details["to_server_ids"] = "1,2,3,4"
    spread_details["commit"] = False
    spread_details["force"] = False
    spread_details["exclude_disabled_servers"] = False

    spread_schedules.main(spread_details, pytest.db_test)

    result = query_test_db(
        f"SELECT server_id, schedule_id, interval_mask, exec_command, is_enabled FROM schedules ORDER BY schedule_id"
    )

    assert result == [
        (1, "1-1", "* * * * *", "1-1", 1),
        (1, "1-2", "* * * * *", "1-2", 1),
        (1, "1-3", "* * * * *", "1-3", 1),
        (1, "1-4", "* * * * *", "1-4", 1),
        (2, "2-1", "* * * * *", "2-1", 1),
        (2, "2-2", "* * * * *", "2-2", 1),
        (2, "2-3", "* * * * *", "2-3", 1),
        (2, "2-4", "* * * * *", "2-4", 1),
        (3, "3-1", "* * * * *", "3-1", 1),
        (3, "3-2", "* * * * *", "3-2", 1),
        (3, "3-3", "* * * * *", "3-3", 1),
        (3, "3-4", "* * * * *", "3-4", 1),
    ]


@freeze_time("2022-01-02 02:00:00")
def test_spread_schedules_commit():
    """test_spread_schedules_commit"""
    spread_details = {}

    spread_details["from_server_ids"] = "1,2,3"
    spread_details["to_server_ids"] = "1,2,3,4"
    spread_details["commit"] = True
    spread_details["force"] = False
    spread_details["exclude_disabled_servers"] = False

    spread_schedules.main(spread_details, pytest.db_test)

    result = query_test_db(
        f"SELECT server_id, schedule_id, is_enabled, abort_running, adhoc_execute FROM schedules ORDER BY schedule_id"
    )

    assert result == [
        (1, "1-1", 1, 0, 0),
        (2, "1-2", 1, 0, 0),
        (3, "1-3", 1, 0, 0),
        (4, "1-4", 1, 0, 0),
        (1, "2-1", 1, 0, 0),
        (2, "2-2", 1, 0, 0),
        (3, "2-3", 1, 0, 0),
        (4, "2-4", 1, 0, 0),
        (1, "3-1", 1, 0, 0),
        (2, "3-2", 1, 0, 0),
        (3, "3-3", 1, 0, 0),
        (4, "3-4", 1, 0, 0),
    ]


@freeze_time("2022-01-02 02:00:00")
def test_spread_schedules_force():
    """test_spread_schedules_force"""
    spread_details = {}

    spread_details["from_server_ids"] = "1,2,3,4"
    spread_details["to_server_ids"] = "1,2,3,4"
    spread_details["commit"] = True
    spread_details["force"] = True
    spread_details["exclude_disabled_servers"] = False

    spread_schedules.main(spread_details, pytest.db_test)

    result = query_test_db(
        f"SELECT server_id, schedule_id, is_enabled, abort_running, adhoc_execute FROM schedules ORDER BY schedule_id"
    )

    assert result == [
        (1, "1-1", 1, 0, 0),
        (2, "1-2", 1, 1, 1),
        (3, "1-3", 1, 0, 0),
        (4, "1-4", 1, 0, 0),
        (1, "2-1", 1, 0, 0),
        (2, "2-2", 1, 0, 0),
        (3, "2-3", 1, 0, 0),
        (4, "2-4", 1, 0, 0),
        (1, "3-1", 1, 0, 0),
        (2, "3-2", 1, 0, 0),
        (3, "3-3", 1, 0, 0),
        (4, "3-4", 1, 0, 0),
    ]


@freeze_time("2022-01-02 02:00:00")
def test_spread_schedules_exclude_disabled_servers():
    """test_spread_schedules_force"""
    spread_details = {}

    spread_details["from_server_ids"] = "1,2,3,4"
    spread_details["to_server_ids"] = "1,2,3,4"
    spread_details["commit"] = True
    spread_details["force"] = True
    spread_details["exclude_disabled_servers"] = True

    spread_schedules.main(spread_details, pytest.db_test)

    result = query_test_db(
        f"SELECT server_id, schedule_id, is_enabled, abort_running, adhoc_execute FROM schedules ORDER BY schedule_id"
    )

    print(result)

    assert result == [
        (4, "1-1", 1, 0, 0),
        (1, "1-2", 1, 1, 1),
        (2, "1-3", 1, 0, 0),
        (4, "1-4", 1, 0, 0),
        (2, "2-1", 1, 0, 0),
        (4, "2-2", 1, 0, 0),
        (1, "2-3", 1, 0, 0),
        (2, "2-4", 1, 0, 0),
        (1, "3-1", 1, 0, 0),
        (2, "3-2", 1, 0, 0),
        (4, "3-3", 1, 0, 0),
        (1, "3-4", 1, 0, 0),
    ]


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
