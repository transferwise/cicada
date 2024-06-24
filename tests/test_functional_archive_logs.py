"""
    test_functional_archive_logs.py
    Test archiving schedule_log entries into schedule_log_historical
"""

from unittest import result
import pytest
import os
import datetime
import psycopg2

from cicada.commands import archive_schedule_log


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


def test_create_dummy_servers():
    """test_create_dummy_servers"""
    query_test_db(
        """
        INSERT INTO servers
            (server_id, hostname, fqdn, ip4_address, is_enabled)
        VALUES
            (1, '1', '1', '192.168.0.1', 1)
        ;
        """
    )

    results = query_test_db(f"SELECT count(*) FROM servers WHERE is_enabled=1")[0][0]

    assert results == 1


def test_create_dummy_schedules():
    """test_create_dummy_schedules"""
    query_test_db(
        """
        INSERT INTO schedules
            (server_id, schedule_id, interval_mask, exec_command, is_enabled, is_running)
        VALUES
             (1, '1-1', '* * * * *', '1-1', 1, 0)
            ,(1, '1-2', '* * * * *', '1-2', 1, 0)
            ,(1, '1-3', '* * * * *', '1-3', 1, 1)
            ,(1, '1-4', '* * * * *', '1-4', 1, 0)
        ;
        """
    )

    results = query_test_db(f"SELECT count(*) FROM schedules WHERE is_enabled=1")[0][0]

    assert results == 4


def test_create_dummy_schedule_logs():
    """test_create_dummy_schedule_logs"""
    query_test_db(
        """
        INSERT INTO schedule_log
            (server_id, schedule_id, full_command, start_time, end_time, returncode)
        VALUES
             (1, '1-1', '1-1', '2022-01-01 01:00:00', '2022-01-01 02:00:00' , 0)
            ,(1, '1-1', '1-1', '2022-01-01 03:00:00', '2022-01-01 04:00:00' , 0)
            ,(1, '1-1', '1-1', '2022-01-01 05:00:00', '2022-01-01 06:00:00' , 0)
            ,(1, '1-1', '1-1', '2022-01-01 07:00:00', '2022-01-01 08:00:00' , 0)
            ,(1, '1-1', '1-1', '2022-01-01 09:00:00', '2022-01-01 10:00:00' , 0)
            ,(1, '1-2', '1-2', '2022-01-01 01:00:00', null , null)
            ,(1, '1-2', '1-2', '2022-01-01 03:00:00', null , null)
            ,(1, '1-2', '1-2', '2022-01-01 04:00:00', null , null)
            ,(1, '1-2', '1-2', '2022-01-01 07:00:00', null , null)
            ,(1, '1-2', '1-2', '2022-01-01 09:00:00', null , null)
            ,(1, '1-3', '1-3', '2022-01-01 01:00:00', null , null)
            ,(1, '1-3', '1-3', '2022-01-01 03:00:00', null , null)
            ,(1, '1-3', '1-3', '2022-01-01 05:00:00', null , null)
            ,(1, '1-3', '1-3', '2022-01-01 07:00:00', null , null)
            ,(1, '1-3', '1-3', '2022-01-01 09:00:00', null , null)
        ;
        """
    )

    results = query_test_db(f"SELECT count(*) FROM schedule_log")[0][0]

    assert results == 15


def test_archive_schedules():
    """test_archive_schedules"""
    archive_schedule_log.main(1, pytest.db_test)

    result = query_test_db(
        f"SELECT (SELECT count(*) FROM schedule_log) || ',' || (SELECT count(*) from schedule_log_historical)"
    )[0][0]

    assert result == "2,13"


def test_archive_schedule_keep_correct_schedules():
    """test_archive_schedule_keep_correct_schedules"""

    result = query_test_db(f"SELECT schedule_id, start_time, end_time, returncode FROM schedule_log")

    assert result == [
        (
            "1-1",
            datetime.datetime(2022, 1, 1, 9, 0),
            datetime.datetime(2022, 1, 1, 10, 0),
            0,
        ),
        ("1-3", datetime.datetime(2022, 1, 1, 9, 0), None, None),
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
