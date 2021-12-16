"""test_main_functionality.py"""

import pytest
import os
import datetime
import psycopg2
import socket

from cicada.lib import postgres
from cicada.commands import register_server
from cicada.commands import upsert_schedule
from cicada.commands import exec_schedule
from cicada.commands import exec_server_schedules


@pytest.fixture(scope="session", autouse=True)
def get_env_vars():
    """get_env_vars"""

    pytest.cicada_home = os.environ.get('CICADA_HOME')

    pytest.db_host = os.environ.get('DB_POSTGRES_HOST')
    pytest.db_port = os.environ.get('DB_POSTGRES_PORT')
    pytest.db_user = os.environ.get('DB_POSTGRES_USER')
    pytest.db_pass = os.environ.get('DB_POSTGRES_PASS')

    pytest.db_test = f"pytest_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"


@pytest.fixture()
def db_setup(get_env_vars):
    """db_setup"""

    # Create the test_db
    pg_conn = psycopg2.connect(
        host = pytest.db_host,
        port = pytest.db_port,
        user = pytest.db_user,
        password = pytest.db_pass,
        database='postgres'
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()
    pg_cur.execute(f"CREATE DATABASE {pytest.db_test}")

    # Create test_db structure
    test_conn = psycopg2.connect(
        host = pytest.db_host,
        port = pytest.db_port,
        user = pytest.db_user,
        password = pytest.db_pass,
        database = pytest.db_test
    )
    test_conn.autocommit = True
    test_cur = test_conn.cursor()
    test_cur.execute(open(f"{pytest.cicada_home}/setup/schema.sql", "r", encoding='utf-8').read())
    test_cur.close()
    test_conn.close()


def query_test_db(query):
    """query_test_db"""
    result_rows = []
    conn = psycopg2.connect(
        host = pytest.db_host,
        port = pytest.db_port,
        user = pytest.db_user,
        password = pytest.db_pass,
        database = pytest.db_test,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query)
    if cur.rowcount > 0:
        result_rows = cur.fetchall()
    cur.close()
    conn.close()

    return result_rows


def update_test_db(query):
    """update_test_db"""
    conn = psycopg2.connect(
        host = pytest.db_host,
        port = pytest.db_port,
        user = pytest.db_user,
        password = pytest.db_pass,
        database = pytest.db_test,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def test_test_db_setup(db_setup):
    """test_test_db_setup"""
    query_result = query_test_db('SELECT 1')

    assert query_result == [(1,)]


def test_register_server():
    """test_register_server"""
    register_server.main(pytest.db_test)

    hostname = socket.gethostname()
    if hostname.find(".") != -1:
        hostname = hostname[:hostname.find(".")]
    fqdn = socket.getfqdn()
    ip4_address = socket.gethostbyname(fqdn)

    results = query_test_db('SELECT hostname, fqdn, ip4_address, is_enabled FROM servers')

    assert (
        results[0][0] == hostname
        and results[0][1] == fqdn
        and results[0][2] == ip4_address
        and results[0][3] == 1
    )


def test_insert_async_schedule():
    """test_insert_async_schedule"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = None
    schedule_details['is_async'] = None
    schedule_details['is_enabled'] = None
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = '--seconds=0.5'
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(f"""
        SELECT schedule_id, schedule_description,
        (SELECT MIN(server_id) FROM servers) AS server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """)

    assert query_result == [ ('pytest', None, 1, 1, 1, 0, 0, '* * * * *', datetime.datetime(1000, 1, 1, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000), '/opt/cicada/venv/bin/cicada wait', '--seconds=0.5',
        None, None, 0), ]


def test_update_schedule():
    """test_update_schedule"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = None
    schedule_details['is_async'] = None
    schedule_details['is_enabled'] = 1
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = '--seconds=0.1'
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db("""
        SELECT schedule_id, schedule_description,
        (SELECT MIN(server_id) FROM servers) AS server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = 'pytest'
        """)

    assert query_result == [ ('pytest', None, 1, 1, 1, 1, 0, '* * * * *', datetime.datetime(1000, 1, 1, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000), '/opt/cicada/venv/bin/cicada wait', '--seconds=0.1',
        None, None, 0), ]


def test_exec_schedule():
    """test_exec_schedule"""
    exec_schedule.main('pytest', pytest.db_test)

    query_result = query_test_db("""SELECT count(*) FROM schedule_log WHERE returncode = 0 AND schedule_id = 'pytest'""")[0][0]

    assert query_result >= 1


def test_insert_adhoc_schedule():
    """test_insert_adhoc_schedule"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest_adhoc'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = None
    schedule_details['is_async'] = None
    schedule_details['is_enabled'] = None
    schedule_details['adhoc_execute'] = 1
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = '--seconds=0.5'
    schedule_details['adhoc_parameters'] = '--seconds=0.1'
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(f"""
        SELECT schedule_id, schedule_description,
        (SELECT MIN(server_id) FROM servers) AS server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """)

    assert query_result == [ ('pytest_adhoc', None, 1, 1, 1, 0, 1, '* * * * *', datetime.datetime(1000, 1, 1, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000), '/opt/cicada/venv/bin/cicada wait', '--seconds=0.5',
        '--seconds=0.1', None, 0), ]


def test_exec_adhoc_schedule():
    """test_exec_adhoc_schedule"""
    exec_schedule.main('pytest_adhoc', pytest.db_test)

    query_result = query_test_db("""SELECT count(*) FROM schedule_log WHERE returncode = 0 AND schedule_id = 'pytest_adhoc'""")[0][0]

    assert query_result >= 1


def test_insert_sync_schedule_1():
    """test_insert_sync_schedule"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest1'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = 1
    schedule_details['is_async'] = 0
    schedule_details['is_enabled'] = 1
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = '--seconds=0.1'
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db("""
        SELECT schedule_id, schedule_description,
        (SELECT MIN(server_id) FROM servers) AS server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = 'pytest1'
        """)

    assert query_result == [ ('pytest1', None, 1, 1, 0, 1, 0, '* * * * *', datetime.datetime(1000, 1, 1, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000), '/opt/cicada/venv/bin/cicada wait', '--seconds=0.1',
        None, None, 0), ]


def test_insert_sync_schedule_2():
    """test_insert_sync_schedule_2"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest2'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = 1
    schedule_details['is_async'] = 0
    schedule_details['is_enabled'] = 1
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = '--seconds=0.1'
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db("""
        SELECT schedule_id, schedule_description,
        (SELECT MIN(server_id) FROM servers) AS server_id,
        schedule_order, is_async, is_enabled, adhoc_execute, interval_mask, first_run_date, last_run_date, exec_command, parameters,
        adhoc_parameters, schedule_group_id, is_running
        FROM schedules WHERE schedule_id = 'pytest2'
        """)

    assert query_result == [ ('pytest2', None, 1, 1, 0, 1, 0, '* * * * *', datetime.datetime(1000, 1, 1, 0, 0),
        datetime.datetime(9999, 12, 31, 23, 59, 59, 999000), '/opt/cicada/venv/bin/cicada wait', '--seconds=0.1',
        None, None, 0), ]


def test_exec_server_schedules():
    """test_exec_server_schedules"""

    exec_server_schedules.main(pytest.db_test)

    pytest1_end_time = query_test_db("""SELECT end_time FROM schedule_log WHERE returncode = 0 AND schedule_id='pytest1'""")[0][0]
    pytest2_start_time = query_test_db("""SELECT start_time FROM schedule_log WHERE returncode = 0 AND schedule_id='pytest2'""")[0][0]

    assert pytest2_start_time > pytest1_end_time


def test_insert_abort_running():
    """test_insert_abort_running"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest_abort_running'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = None
    schedule_details['is_async'] = None
    schedule_details['is_enabled'] = None
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = 1
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = '--seconds=600'
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(f"""
        SELECT schedule_id, exec_command, parameters, is_running, abort_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """)

    assert query_result == [ ('pytest_abort_running', '/opt/cicada/venv/bin/cicada wait', '--seconds=600', 0, 1), ]


# @pytest.mark.skip(reason="not ready yet")
def test_exec_abort_running():
    """test_exec_abort_running"""
    exec_schedule.main('pytest_abort_running', pytest.db_test)
    # update_test_db("""UPDATE schedules SET abort_running=1 WHERE schedule_id='pytest_abort_running'""")

    query_result = query_test_db("""SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_abort_running'""")

    assert query_result == [('pytest_abort_running', -15, 'SIGTERM by Cicada')]


def test_insert_faulty_schedule_1():
    """test_insert_faulty_schedule_1"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest_faulty_1'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = None
    schedule_details['is_async'] = None
    schedule_details['is_enabled'] = None
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = '/opt/cicada/venv/bin/cicada wait'
    schedule_details['parameters'] = None
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(f"""
        SELECT schedule_id, exec_command, parameters, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """)

    assert query_result == [ ('pytest_faulty_1', '/opt/cicada/venv/bin/cicada wait', None, 0), ]


def test_exec_faulty_schedule_1():
    """test_exec_faulty_schedule_1"""
    exec_schedule.main('pytest_faulty_1', pytest.db_test)

    query_result = query_test_db("""SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_faulty_1'""")

    assert query_result == [('pytest_faulty_1', 2, None)]


def test_insert_faulty_schedule_2():
    """test_insert_faulty_schedule_2"""

    schedule_details = {}
    schedule_details['schedule_id'] = 'pytest_faulty_2'
    schedule_details['schedule_description'] = None
    schedule_details['server_id'] = None
    schedule_details['schedule_order'] = None
    schedule_details['is_async'] = None
    schedule_details['is_enabled'] = None
    schedule_details['adhoc_execute'] = None
    schedule_details['is_running'] = None
    schedule_details['abort_running'] = None
    schedule_details['interval_mask'] = '* * * * *'
    schedule_details['first_run_date'] = None
    schedule_details['last_run_date'] = None
    schedule_details['exec_command'] = 'fake.exe'
    schedule_details['parameters'] = None
    schedule_details['adhoc_parameters'] = None
    schedule_details['schedule_group_id'] = None

    upsert_schedule.main(schedule_details, pytest.db_test)

    query_result = query_test_db(f"""
        SELECT schedule_id, exec_command, parameters, is_running
        FROM schedules WHERE schedule_id = '{schedule_details['schedule_id']}'
        """)

    assert query_result == [ ('pytest_faulty_2', 'fake.exe', None, 0), ]


def test_exec_faulty_schedule_2():
    """test_exec_faulty_schedule_2"""
    exec_schedule.main('pytest_faulty_2', pytest.db_test)

    query_result = query_test_db("""SELECT schedule_id, returncode, error_detail FROM schedule_log WHERE schedule_id = 'pytest_faulty_2'""")

    print(query_result)

    assert query_result == [('pytest_faulty_2', 2, 'No such file or directory')]


def test_db_teardown():
    """test_db_teardown"""
    pg_conn = psycopg2.connect(
        host = pytest.db_host,
        port = pytest.db_port,
        user = pytest.db_user,
        password = pytest.db_pass,
        database='postgres'
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    pg_cur.execute(f"DROP DATABASE IF EXISTS {pytest.db_test}")
    pg_cur.close()
    pg_conn.close()
