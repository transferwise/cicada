"""Tests for smart scheduling and rollback functionality"""

import croniter
import pytest
import os
import datetime
from unittest.mock import Mock, MagicMock, patch, call
import numpy as np
import psycopg2

from cicada.lib.SmartScheduling.domain import Schedule
from cicada.lib.SmartScheduling.config import GAConfig
from cicada.lib.SmartScheduling.evaluation import evaluate_usage_and_peak
import cicada.commands.smart_schedule as smart_schedule
from cicada.lib.SmartScheduling.GAPyGAD import GAPyGADScheduler
from cicada.lib import scheduler


@pytest.fixture(scope="session", autouse=True)
def get_env_vars():
    """get_env_vars"""

    pytest.cicada_home = os.environ.get("CICADA_HOME")

    pytest.db_host = os.environ.get("DB_POSTGRES_HOST")
    pytest.db_port = os.environ.get("DB_POSTGRES_PORT")
    pytest.db_user = os.environ.get("DB_POSTGRES_USER")
    pytest.db_pass = os.environ.get("DB_POSTGRES_PASS")


@pytest.fixture()
def db_setup(get_env_vars):
    """db_setup"""
    pytest.db_test = f"pytest_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

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
    pg_cur.close()
    pg_conn.close()

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

    yield

    # Cleanup: terminate all connections and drop test database
    pg_conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database="postgres",
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()
    # Terminate all connections to the test database
    pg_cur.execute(
        f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{pytest.db_test}'
        AND pid <> pg_backend_pid()
        """
    )
    pg_cur.close()
    pg_conn.close()

    # Now drop the database
    pg_conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database="postgres",
    )
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()
    pg_cur.execute(f"DROP DATABASE {pytest.db_test}")
    pg_cur.close()
    pg_conn.close()


def query_test_db(query):
    """Run a SQL query in a postgres database"""
    rows = []
    conn = None
    try:
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
    finally:
        if conn:
            conn.close()
    return rows


def get_db_cursor():
    """Get a cursor to the test database"""
    conn = psycopg2.connect(
        host=pytest.db_host,
        port=pytest.db_port,
        user=pytest.db_user,
        password=pytest.db_pass,
        database=pytest.db_test,
    )
    conn.set_session(readonly=False, autocommit=True)
    return conn, conn.cursor()


class TestEvaluateUsageAndPeak:
    """Tests for evaluate_usage_and_peak function"""

    def test_evaluate_single_schedule_no_overlap(self, db_setup):
        """Test usage evaluation with a single schedule that doesn't overlap"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": 1,
                "server_id": 1,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            start_blocks = [0]
            usage, peak = evaluate_usage_and_peak(start_blocks, [test_schedule])

            assert usage.shape == (1440,)
            assert peak == 1
            for i in range(24):
                mins = i * 60
                assert (usage[mins : mins + 5] == 1).all()
                assert (usage[mins + 5 : (i + 1) * 60] == 0).all()
        finally:
            db_cur.close()
            db_conn.close()

    def test_evaluate_multiple_schedules_no_overlap(self, db_setup):
        """Test evaluation with multiple schedules that don't overlap"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule1_details = {
                "schedule_id": 1,
                "server_id": 1,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            schedule1 = Schedule(
                schedule_id=schedule1_details['schedule_id'],
                server_id=schedule1_details['server_id'],
                interval_mask=schedule1_details['interval_mask'],
                smart_interval_mask=schedule1_details.get('smart_interval_mask'),
                blocklisted=schedule1_details.get('blocklisted'),
                db_cur=db_cur
            )
            schedule1.frequency_minutes = 60
            schedule1.median_runtime_minutes = 5

            schedule2_details = {
                "schedule_id": 2,
                "server_id": 1,
                "interval_mask": "30 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            schedule2 = Schedule(
                schedule_id=schedule2_details['schedule_id'],
                server_id=schedule2_details['server_id'],
                interval_mask=schedule2_details['interval_mask'],
                smart_interval_mask=schedule2_details.get('smart_interval_mask'),
                blocklisted=schedule2_details.get('blocklisted'),
                db_cur=db_cur
            )
            schedule2.frequency_minutes = 60
            schedule2.median_runtime_minutes = 5

            start_blocks = [0, 30]
            usage, peak = evaluate_usage_and_peak(start_blocks, [schedule1, schedule2])

            assert (usage[0:5] == 1).all()
            assert (usage[6:30] == 0.0).all()
            assert (usage[30:35] == 1).all()
            assert (usage[35:60] == 0.0).all()
            assert peak == 1
        finally:
            db_cur.close()
            db_conn.close()

    def test_evaluate_overlapping_schedules(self, db_setup):
        """Test evaluation with overlapping schedules"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule1_details = {
                "schedule_id": 1,
                "server_id": 1,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            schedule1 = Schedule(
                schedule_id=schedule1_details['schedule_id'],
                server_id=schedule1_details['server_id'],
                interval_mask=schedule1_details['interval_mask'],
                smart_interval_mask=schedule1_details.get('smart_interval_mask'),
                blocklisted=schedule1_details.get('blocklisted'),
                db_cur=db_cur
            )
            schedule1.frequency_minutes = 60
            schedule1.median_runtime_minutes = 10

            schedule2_details = {
                "schedule_id": 2,
                "server_id": 1,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            schedule2 = Schedule(
                schedule_id=schedule2_details['schedule_id'],
                server_id=schedule2_details['server_id'],
                interval_mask=schedule2_details['interval_mask'],
                smart_interval_mask=schedule2_details.get('smart_interval_mask'),
                blocklisted=schedule2_details.get('blocklisted'),
                db_cur=db_cur
            )
            schedule2.frequency_minutes = 60
            schedule2.median_runtime_minutes = 5

            start_blocks = [0, 0]
            usage, peak = evaluate_usage_and_peak(start_blocks, [schedule1, schedule2])

            assert peak == 2
            assert usage[0] == 2
            assert usage[5] == 1
        finally:
            db_cur.close()
            db_conn.close()

    def test_evaluate_wrapping_around_day(self, db_setup):
        """Test that schedules wrapping around midnight work correctly"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": 1,
                "server_id": 1,
                "interval_mask": "0 0 * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )
            test_schedule.frequency_minutes = 60
            test_schedule.median_runtime_minutes = 5
            start_blocks = [1430]  # (1430 mins = 23:50)

            # Should throw an assertion error that the start block is too late for the frequency of the schedule
            with pytest.raises(ValueError):
                evaluate_usage_and_peak(start_blocks, [test_schedule])
        finally:
            db_cur.close()
            db_conn.close()



class TestScheduleDomain:
    """Tests for Schedule domain object"""

    def test_schedule_initialization(self, db_setup):
        """Test Schedule object initialization"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 5,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.schedule_id == "test-id-1"
            assert test_schedule.server_id == 5
            assert test_schedule.interval_mask == "0 * * * *"
            assert test_schedule.shifted == False
            assert test_schedule.start_time_mins == 0
            assert test_schedule.median_runtime_minutes == 5
        finally:
            db_cur.close()
            db_conn.close()


    def test_schedule_dataclass_fields_initialized(self, db_setup):
        """Test that all dataclass fields are properly initialized, including defaults"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 5,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            # Verify all fields exist and have correct default values
            assert hasattr(test_schedule, 'shifted')
            assert hasattr(test_schedule, 'median_runtime_minutes')
            assert hasattr(test_schedule, 'start_time_mins')
            assert hasattr(test_schedule, 'blocklisted')
            assert hasattr(test_schedule, 'frequency_minutes')

            # Verify default values
            assert test_schedule.shifted is False
            assert test_schedule.median_runtime_minutes == 5  # Will be updated by _get_average_runtime
            assert test_schedule.start_time_mins == 0
            assert test_schedule.blocklisted is False

            # Accessing any of these should NOT raise AttributeError
            try:
                _ = test_schedule.shifted
                _ = test_schedule.median_runtime_minutes
                _ = test_schedule.start_time_mins
                _ = test_schedule.blocklisted
            except AttributeError as e:
                pytest.fail(f"AttributeError raised when accessing dataclass field: {e}")
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_frequency_hourly(self, db_setup):
        """Test frequency determination for hourly cron"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0 * * * *",  # Every hour
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.frequency_minutes == 60
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_frequency_daily(self, db_setup):
        """Test frequency determination for daily cron"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0 0 * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.frequency_minutes == 1440
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_is_unsupported_irregular_cron(self, db_setup):
        """Test that schedules with irregular cron expressions are marked as unsupported"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0-15 */9 * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.is_unsupported()
            assert not test_schedule.frequency_is_supported()
            assert not test_schedule.is_regular_schedule()
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_is_unsupported_low_frequency(self, db_setup):
        """Test that schedules with unsupported low frequencies are marked as unsupported"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0 0 * * 0",  # Weekly
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.is_unsupported()
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_is_regular_schedule_hourly(self, db_setup):
        """Test that hourly schedules are recognized as regular"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.is_regular_schedule()
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_is_regular_schedule_every_15_mins(self, db_setup):
        """Test that every-15-minute schedules are recognized as regular"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "*/15 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.is_regular_schedule()
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_is_regular_schedule_daily(self, db_setup):
        """Test that daily schedules are recognized as regular"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0 0 * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert test_schedule.is_regular_schedule()
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_45_min_schedule_is_supported(self, db_setup):
        """Test that 45-minute frequency schedules are recognized as supported"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "*/45 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert not test_schedule.is_unsupported()
            # Fails due to cronitor issue -> means any */45 gets missed out of the smart scheduling
        finally:
            db_cur.close()
            db_conn.close()

    def test_schedule_is_irregular_schedule_weekdays(self, db_setup):
        """Test that weekday-only schedules are marked as irregular"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-id-1",
                "server_id": 1,
                "interval_mask": "0 9 * * 1-5",  # Weekdays only
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )

            assert not test_schedule.is_regular_schedule()
        finally:
            db_cur.close()
            db_conn.close()


class TestGAPyGADScheduler:
    """Tests for GAPyGADScheduler"""

    def test_custom_config(self):
        """Test GAConfig with custom values"""
        config = GAConfig(
            num_generations=50,
            sol_per_pop=100,
            random_seed=42,
        )
        assert config.num_generations == 50
        assert config.sol_per_pop == 100
        assert config.random_seed == 42
        assert config.num_parents_mating == 10
        assert config.mutation_percent_genes == 20
        assert config.parent_selection_type == "rank"
        assert config.crossover_type == "uniform"
        assert config.mutation_type == "random"

    def test_scheduler_uses_default_config_when_optional_config_is_missing(self):
        ga_scheduler = GAPyGADScheduler()

        assert ga_scheduler.cfg == GAConfig()
        assert ga_scheduler.cfg.num_generations == 20

    def test_scheduler_initialization_custom_config(self):
        """Test scheduler initialization with custom config"""
        config = {"num_generations": 30}
        ga_scheduler = GAPyGADScheduler(config)

        assert ga_scheduler.cfg.num_generations == 30

    def test_scheduler_initialization_filters_none_values(self):
        """Test that None values are filtered out when initializing config"""
        config = {"num_generations": None}
        ga_scheduler = GAPyGADScheduler(config)

        assert ga_scheduler.cfg.num_generations == 20


class TestSchedulerDatabaseFunctions:
    """Tests for scheduler database functions (rollback/snapshot)"""

    def test_get_blocklisted_schedule_ids_empty(self, db_setup):
        """Test retrieving blocklisted schedule IDs when none exist"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Initially should have the 10 admin schedules
            result = scheduler.get_blocklisted_schedule_ids(db_cur)
            assert len(result) == 18
        finally:
            db_cur.close()
            db_conn.close()

    def test_blocklist_schedule(self, db_setup):
        """Test blocklisting a schedule"""
        db_conn, db_cur = get_db_cursor()
        try:
            # First register a server and create a schedule
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'G')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, exec_command)
                   VALUES ('test-sched-1', 1, '0 * * * *', 'echo test')"""
            )

            # Blocklist the schedule
            scheduler.blocklist_schedule(db_cur, "test-sched-1", reason="Testing")

            # Verify it's blocklisted
            result = scheduler.get_blocklisted_schedule_ids(db_cur)
            assert len(result) >= 1
            assert "test-sched-1" in result
        finally:
            db_cur.close()
            db_conn.close()

    def test_snapshot_schedules_basic(self, db_setup):
        """Test snapshotting schedules"""
        db_conn, db_cur = get_db_cursor()      
        query_test_db("DELETE FROM schedules")

        try:
            # Register a server and create schedules
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'G')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-sched-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )

            # Snapshot the schedule
            scheduler.snapshot_schedules(db_cur, server_id=1, reason="Test optimization")

            # Verify snapshot was created
            snapshots = query_test_db("SELECT snapshot_id FROM snapshots WHERE reason = 'Test optimization'")
            assert len(snapshots) > 0
        finally:
            db_cur.close()
            db_conn.close()

    def test_full_rollback_with_schedule_id(self, db_setup):
        """Test full rollback for a specific schedule"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Register a server and create a schedule with smart_interval_mask set
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'B')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-sched-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )

            # Perform full rollback
            scheduler.full_rollback(db_cur, schedule_id="test-sched-1")

            # Verify smart_interval_mask is set to NULL
            result = query_test_db(
                "SELECT smart_interval_mask FROM schedules WHERE schedule_id = 'test-sched-1'"
            )
            assert result[0][0] is None
        finally:
            db_cur.close()
            db_conn.close()

    def test_restore_previous_schedules_requires_snapshot_id(self):
        """Test that restore_previous_schedules requires snapshot_id"""
        db_cur = Mock()

        with pytest.raises(TypeError):
            scheduler.restore_previous_schedules(db_cur)


class TestEndToEndSmartScheduling:
    """Integration tests for end-to-end smart scheduling workflow"""

    def test_create_schedules_from_details(self, db_setup):
        """Test creating multiple Schedule objects from details"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedules_data = [
                {
                    "schedule_id": "sched-1",
                    "server_id": 1,
                    "interval_mask": "0 * * * *",
                    "smart_interval_mask": None,
                    "blocklisted": False
                },
                {
                    "schedule_id": "sched-2",
                    "server_id": 1,
                    "interval_mask": "*/30 * * * *",
                    "smart_interval_mask": None,
                    "blocklisted": False
                },
            ]

            schedules = [Schedule(
                schedule_id=data['schedule_id'],
                server_id=data['server_id'],
                interval_mask=data['interval_mask'],
                smart_interval_mask=data.get('smart_interval_mask'),
                blocklisted=data.get('blocklisted'),
                db_cur=db_cur
            ) for data in schedules_data]

            assert len(schedules) == 2
            assert schedules[0].schedule_id == "sched-1"
            assert schedules[1].schedule_id == "sched-2"
        finally:
            db_cur.close()
            db_conn.close()

    def test_snapshot_schedules(self, db_setup):
        """Test the snapshot_schedules function"""
        db_conn, db_cur = get_db_cursor()
        try:
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('sched-1', 1, '0 * * * *', '0 * * * *', 'echo test')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('sched-2', 1, '*/30 * * * *', '*/30 * * * *', 'echo test')"""
            )

            scheduler.snapshot_schedules(db_cur, server_id=1, reason="Test optimization")

            # Verify that snapshots were created
            snapshots = query_test_db("SELECT snapshot_id FROM snapshots WHERE reason = 'Test optimization'")
            assert len(snapshots) > 0
        finally:
            db_cur.close()
            db_conn.close()

    def test_retrieve_snapshots(self, db_setup):
        """Test retrieving schedule snapshots"""
        db_conn, db_cur = get_db_cursor()
        try:
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-sched', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )
            query_test_db(
                """INSERT INTO snapshots (snapshot_id, server_id, reason, snapshot_timestamp)
                   VALUES (1, 1, 'GA optimization', NOW())"""
            )

            snapshots = scheduler.retrieve_snapshots(db_cur, 1)

            assert len(snapshots) > 0
        finally:
            db_cur.close()
            db_conn.close()

    def test_multiple_overlapping_schedules_evaluation(self, db_setup):
        """Test evaluating usage for multiple overlapping schedules"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Create 3 schedules with different patterns
            schedules = []
            for i in range(3):
                schedule_data = {
                    "schedule_id": f"sched-{i}",
                    "server_id": 1,
                    "interval_mask": "0 * * * *" if i == 0 else f"*/{15 * (i + 1)} * * * *",
                    "smart_interval_mask": None,
                    "blocklisted": False
                }
                test_schedule = Schedule(
                    schedule_id=schedule_data['schedule_id'],
                    server_id=schedule_data['server_id'],
                    interval_mask=schedule_data['interval_mask'],
                    smart_interval_mask=schedule_data.get('smart_interval_mask'),
                    blocklisted=schedule_data.get('blocklisted'),
                    db_cur=db_cur
                )
                test_schedule.frequency_minutes = 60
                test_schedule.median_runtime_minutes = 5
                schedules.append(test_schedule)

            # Stagger start times to create overlaps
            start_blocks = [0, 10, 20]
            usage, peak = evaluate_usage_and_peak(start_blocks, schedules)

            assert peak > 0.3  # Should have some overlapping usage
            assert usage.shape == (1440,)
        finally:
            db_cur.close()
            db_conn.close()


class TestSmartSchedulingCommand:
    """Tests for the smart scheduling command"""

    def test_smart_scheduling_frequency_unchanged_hourly_schedule(self, db_setup):
        """Test that the frequency of the schedule remains unchanged after smart scheduling"""
        db_conn, db_cur = get_db_cursor()
        try:
            hourly_schedule_details = {
                "schedule_id": "test-schedule-1",
                "server_id": 1,
                "interval_mask": "0 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            hourly_schedule = Schedule(
                schedule_id=hourly_schedule_details['schedule_id'],
                server_id=hourly_schedule_details['server_id'],
                interval_mask=hourly_schedule_details['interval_mask'],
                smart_interval_mask=hourly_schedule_details.get('smart_interval_mask'),
                blocklisted=hourly_schedule_details.get('blocklisted'),
                db_cur=db_cur
            )
            hourly_schedule.shifted = True
            hourly_schedule.start_time_mins = 15

            smart_schedule._update_schedule_cron(hourly_schedule)
            assert hourly_schedule.smart_interval_mask == "15 * * * *"
            assert hourly_schedule.interval_mask == "0 * * * *"
            assert hourly_schedule.frequency_minutes == 60

            hourly_schedule.determine_attributes(db_cur)
            assert hourly_schedule.is_regular_schedule()
            assert hourly_schedule.frequency_minutes == 60
        finally:
            db_cur.close()
            db_conn.close()

    def test_smart_scheduling_frequency_unchanged_fifteen_min_schedule(self, db_setup):
        """Test that the frequency of the schedule remains unchanged after smart scheduling"""
        db_conn, db_cur = get_db_cursor()
        try:
            fifteen_min_schedule_details = {
                "schedule_id": "test-schedule-2",
                "server_id": 1,
                "interval_mask": "*/15 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            fifteen_min_schedule = Schedule(
                schedule_id=fifteen_min_schedule_details['schedule_id'],
                server_id=fifteen_min_schedule_details['server_id'],
                interval_mask=fifteen_min_schedule_details['interval_mask'],
                smart_interval_mask=fifteen_min_schedule_details.get('smart_interval_mask'),
                blocklisted=fifteen_min_schedule_details.get('blocklisted'),
                db_cur=db_cur
            )
            fifteen_min_schedule.shifted = True
            fifteen_min_schedule.start_time_mins = 3

            smart_schedule._update_schedule_cron(fifteen_min_schedule)
            assert fifteen_min_schedule.smart_interval_mask == "3-59/15 * * * *"
            assert fifteen_min_schedule.frequency_minutes == 15

            fifteen_min_schedule.determine_attributes(db_cur)
            assert fifteen_min_schedule.is_regular_schedule()
            assert fifteen_min_schedule.frequency_minutes == 15
        finally:
            db_cur.close()
            db_conn.close()

    def test_gene_space_constraints(self, db_setup):
        """Test that the gene space constraints are respected when updating schedule crons"""
        db_conn, db_cur = get_db_cursor()
        try:
            schedule_details = {
                "schedule_id": "test-schedule-3",
                "server_id": 1,
                "interval_mask": "*/45 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )
            test_schedule.frequency_minutes = 45
            test_schedule.shifted = True
            test_schedule.start_time_mins = 50  # Shift greater than frequency

            with pytest.raises(ValueError):
                smart_schedule._update_schedule_cron(test_schedule)
        finally:
            db_cur.close()
            db_conn.close()

    def test_smart_scheduling_gene_space_constraints_30_min(self, db_setup):
        """Test that the gene space constraints don't create invalid cron expressions"""
        db_conn, db_cur = get_db_cursor()
        try:
            ga_scheduler = GAPyGADScheduler()

            schedule_details = {
                "schedule_id": "test-schedule-1",
                "server_id": 1,
                "interval_mask": "*/30 * * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )
            gene_space = ga_scheduler._gene_space([test_schedule])

            test_schedule.shifted = True
            test_schedule.start_time_mins = gene_space[0]["high"]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "29-59/30 * * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 30

            test_schedule.start_time_mins = gene_space[0]["low"] + 1
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "1-59/30 * * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 30
        finally:
            db_cur.close()
            db_conn.close()

    def test_smart_scheduling_gene_space_constraints_daily(self, db_setup):
        """Test that the gene space constraints don't create invalid cron expressions"""
        db_conn, db_cur = get_db_cursor()
        try:
            ga_scheduler = GAPyGADScheduler()

            schedule_details = {
                "schedule_id": "test-schedule-4",
                "server_id": 1,
                "interval_mask": "30 8 * * *",
                "smart_interval_mask": None,
                "blocklisted": False
            }
            test_schedule = Schedule(
                schedule_id=schedule_details['schedule_id'],
                server_id=schedule_details['server_id'],
                interval_mask=schedule_details['interval_mask'],
                smart_interval_mask=schedule_details.get('smart_interval_mask'),
                blocklisted=schedule_details.get('blocklisted'),
                db_cur=db_cur
            )
            gene_space = ga_scheduler._gene_space([test_schedule])

            test_schedule.shifted = True
            test_schedule.start_time_mins = gene_space[0]["high"]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "29 9 * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 1440

            test_schedule.start_time_mins = gene_space[0]["low"]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "30 8 * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 1440
        finally:
            db_cur.close()
            db_conn.close()

    def test_get_schedules_per_server_no_schedules_single_server(self, db_setup):
        """Test that _get_schedules_per_server raises ValueError when no schedules exist for a server"""
        try:
            # Create two servers (omit server_id to use auto-increment)
            query_test_db(
                """INSERT INTO servers (hostname, fqdn, ip4_address)
                   VALUES ('test-server-1', 'test-server-1.local', '127.0.0.1'),
                          ('test-server-2', 'test-server-2.local', '127.0.0.2')"""
            )

            # Add a schedule only to server 1
            query_test_db(
                """INSERT INTO schedules
                   (schedule_id, server_id, interval_mask, exec_command)
                   VALUES ('schedule-1', 1, '0 * * * *', 'echo test')"""
            )

            # Get a fresh cursor after data insertion
            db_conn, db_cur = get_db_cursor()

            # Attempt to get schedules for server 2 without any schedules
            with pytest.raises(ValueError, match="No schedules found for server_id 2"):
                smart_schedule._get_schedules_per_server(server_id=2, db_cur=db_cur)

            db_cur.close()
            db_conn.close()
        except Exception as e:
            raise e

    def test_main_no_schedules_single_server(self, db_setup, capsys):
        """Test that main() handles servers without schedules gracefully (single server)"""
        try:
            # Create two servers
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server-1', 'test-server-1.local', '127.0.0.1'),
                          (2, 'test-server-2', 'test-server-2.local', '127.0.0.2')"""
            )

            # Add a schedule only to server 1
            query_test_db(
                """INSERT INTO schedules
                   (schedule_id, server_id, interval_mask, exec_command)
                   VALUES ('schedule-1', 1, '0 * * * *', 'echo test')"""
            )

            # Call main with server_id 2 (should return early without error)
            smart_schedule.main(server_id=2, dbname=pytest.db_test)

            # Verify that ValueError message was printed
            captured = capsys.readouterr()
            assert "No schedules found for server_id 2" in captured.out
        except Exception as e:
            raise e


class TestScheduleSnapshots:
    """Tests for schedule snapshots functionality"""

    def test_snapshot_schedules(self, db_setup):
        """Test snapshotting schedules and automatic schedule backup creation"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Create server and schedules first
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'C')"""
            )
            schedule_ids = ["test-schedule-1", "test-schedule-2"]
            for sched_id in schedule_ids:
                query_test_db(
                    f"""INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                       VALUES ('{sched_id}', 1, '0 * * * *', '30 * * * *', 'echo test')"""
                )

            # Snapshot the schedules
            scheduler.snapshot_schedules(db_cur, server_id = 1, reason="Test optimization")

            # Verify snapshot was created
            snapshot_result = query_test_db("SELECT snapshot_id, server_id FROM snapshots WHERE reason = 'Test optimization'")
            assert len(snapshot_result) > 0
            snapshot_id = snapshot_result[0][0]
            server_id = snapshot_result[0][1]
            assert server_id == 1

            # Verify schedule backups exist for this snapshot
            schedule_backups_result = query_test_db("SELECT schedule_id FROM schedule_backups WHERE snapshot_id = %s" % snapshot_id)
            assert len(schedule_backups_result) == len(schedule_ids)
            backup_schedule_ids = [row[0] for row in schedule_backups_result]
            for schedule_id in schedule_ids:
                assert schedule_id in backup_schedule_ids

        finally:
            db_cur.close()
            db_conn.close()
        

    def test_full_rollback_by_server_id(self, db_setup):
        """Test full rollback for a server"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Register a server and create a schedule with smart_interval_mask set
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'D')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-schedule-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )
            scheduler.full_rollback(db_cur, server_id=1)
            assert query_test_db("SELECT smart_interval_mask FROM schedules WHERE schedule_id = 'test-schedule-1'")[0][0] is None
            assert query_test_db("SELECT interval_mask FROM schedules WHERE schedule_id = 'test-schedule-1'")[0][0] == "0 * * * *"


        finally:
            db_cur.close()
            db_conn.close()


    def test_full_rollback_by_schedule_id(self, db_setup):
        """Test full rollback for a specific schedule"""
        db_conn, db_cur = get_db_cursor()
        try:
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'E')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-schedule-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )

            # Perform full rollback
            scheduler.full_rollback(db_cur, schedule_id="test-schedule-1")

            # Verify smart_interval_mask is set to NULL
            assert query_test_db("SELECT smart_interval_mask FROM schedules WHERE schedule_id = 'test-schedule-1'")[0][0] is None
            assert query_test_db("SELECT interval_mask FROM schedules WHERE schedule_id = 'test-schedule-1'")[0][0] == "0 * * * *"

        finally:
            db_cur.close()
            db_conn.close()

    def test_restore_previous_schedules(self, db_setup):
        """Test rollback to previous for a specific schedule"""
        db_conn, db_cur = get_db_cursor()
        try:
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'F')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-schedule-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )
            scheduler.snapshot_schedules(db_cur, server_id=1, reason="Test optimization")
            assert query_test_db("SELECT smart_interval_mask FROM schedule_backups WHERE schedule_id = 'test-schedule-1'")[0][0] == "30 * * * *"
            assert query_test_db("SELECT COUNT(*) FROM schedule_backups WHERE schedule_id = 'test-schedule-1'")[0][0] == 1

            query_test_db("UPDATE schedules SET smart_interval_mask = '45 * * * *' WHERE schedule_id = 'test-schedule-1'")

            scheduler.snapshot_schedules(db_cur, server_id=1, reason="Test optimization")
            assert query_test_db("SELECT server_id FROM snapshots ORDER BY snapshot_id DESC LIMIT 1")[0][0] == 1
            assert query_test_db("SELECT smart_interval_mask FROM schedule_backups WHERE schedule_id = 'test-schedule-1' ORDER BY snapshot_id DESC LIMIT 1")[0][0] == "45 * * * *"
            assert query_test_db("SELECT COUNT(*) FROM schedule_backups WHERE schedule_id = 'test-schedule-1'")[0][0] == 2

            # Perform rollback to previous snapshot
            prev_snapshot_id = query_test_db("SELECT snapshot_id FROM snapshots ORDER BY snapshot_timestamp DESC LIMIT 1 OFFSET 1")[0][0]
            scheduler.restore_previous_schedules(db_cur, snapshot_id=prev_snapshot_id, server_id=1)
            assert query_test_db("SELECT smart_interval_mask FROM schedules WHERE schedule_id = 'test-schedule-1'")[0][0] == "30 * * * *"

        finally:
            db_cur.close()
            db_conn.close()


    def test_snapshot_cleanup(self, db_setup):
        """Test that snapshot limits are enforced and old snapshots are deleted"""
        db_conn, db_cur = get_db_cursor()
        try:
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', 'H')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('test-schedule-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )
            # Create more than 5 snapshots to trigger deletion of old snapshots
            for i in range(7):
                scheduler.snapshot_schedules(db_cur, server_id=1)

            # Verify that only the 5 most recent snapshots remain
            snapshot_count = query_test_db("SELECT COUNT(*) FROM snapshots WHERE server_id = 1")[0][0]
            assert snapshot_count == 5
            oldest_snapshot_id = query_test_db("SELECT snapshot_id FROM snapshots WHERE server_id = 1 ORDER BY snapshot_timestamp ASC LIMIT 1")[0][0]
            assert oldest_snapshot_id == 3
            oldest_snapshot_id = query_test_db("SELECT snapshot_id FROM schedule_backups WHERE server_id = 1 ORDER BY snapshot_id ASC LIMIT 1")[0][0]
            assert oldest_snapshot_id == 3

        finally:
            db_cur.close()
            db_conn.close()


class TestOptimiseWithCustomDbConnection:
    """Tests for optimise() function with custom db connection"""

    def test_optimise_with_custom_db_connection_single_server(self, db_setup):
        """Test optimise() function using a custom db connection for a single server"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create server and schedules
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, exec_command)
                   VALUES ('sched-1', 1, '*/10 * * * *', 'echo test1'),
                          ('sched-2', 1, '0 * * * *', 'echo test2'),
                          ('sched-3', 1, '0 * * * *', 'echo test3'),
                          ('sched-4', 1, '0 * * * *', 'echo test4'),
                          ('sched-5', 1, '0 * * * *', 'echo test5')"""
            )

            # Call optimise with custom db_cur
            ga_config = {"random_seed": 1, "mutation_type": None, "num_generations": 2, "sol_per_pop": 5,  "num_parents_mating": 2}
            smart_schedule.optimise(db_cur=db_cur, server_id=1, ga_config=ga_config)

            # Verify that the schedules were processed (no errors should occur)
            schedules = query_test_db("SELECT schedule_id FROM schedules WHERE server_id = 1")
            smart_interval_masks = query_test_db("SELECT smart_interval_mask FROM schedules WHERE server_id = 1")
            snapshots = query_test_db("SELECT snapshot_id FROM snapshots WHERE server_id = 1")
            schedule_backups = query_test_db("SELECT schedule_id, interval_mask, smart_interval_mask FROM schedule_backups WHERE server_id = 1")
            assert all(mask is not None for mask in smart_interval_masks)
            assert query_test_db("""SELECT count(*) FROM schedules 
                                 LEFT JOIN schedule_backups ON schedules.schedule_id = schedule_backups.schedule_id 
                                 WHERE schedules.interval_mask != schedule_backups.interval_mask
                                 OR schedules.smart_interval_mask != schedule_backups.smart_interval_mask""")[0][0]  == 0


            assert len(schedules) == 5
            assert len(snapshots) == 1
            assert len(schedule_backups) == 5
        finally:
            db_cur.close()
            db_conn.close()
            

    def test_optimise_with_custom_db_connection_multiple_servers(self, db_setup):
        """Test optimise() function with custom db connection for multiple servers"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create multiple servers and schedules
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'server-1', 'server-1.local', '192.168.1.1'),
                          (2, 'server-2', 'server-2.local', '192.168.1.2')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, exec_command)
                   VALUES ('sched-1a', 1, '*/10 * * * *', 'echo test1'),
                          ('sched-2a', 1, '0 * * * *', 'echo test2'),
                          ('sched-3a', 1, '0 * * * *', 'echo test3'),
                          ('sched-4a', 1, '0 * * * *', 'echo test4'),
                          ('sched-5a', 1, '0 * * * *', 'echo test5'),
                          ('sched-1b', 2, '*/10 * * * *', 'echo test1'),
                          ('sched-2b', 2, '0 * * * *', 'echo test2'),
                          ('sched-3b', 2, '0 * * * *', 'echo test3'),
                          ('sched-4b', 2, '0 * * * *', 'echo test4'),
                          ('sched-5b', 2, '0 * * * *', 'echo test5')
                          """
            )

            # Call optimise with custom db_cur
            ga_config = {"random_seed": 1, "mutation_type": None, "num_generations": 2, "sol_per_pop": 5,  "num_parents_mating": 2}

            results_1 = query_test_db("SELECT schedule_id, smart_interval_mask FROM schedules WHERE server_id = 1")
            results_2 = query_test_db("SELECT schedule_id, smart_interval_mask FROM schedules WHERE server_id = 2")
            if results_1:
                schedules_1, smart_interval_masks_1 = zip(*results_1)
            if results_2:
                schedules_2, smart_interval_masks_2 = zip(*results_2)
            smart_schedule.optimise(db_cur=db_cur, ga_config=ga_config)

            results_1 = query_test_db("SELECT schedule_id, smart_interval_mask FROM schedules WHERE server_id = 1")
            results_2 = query_test_db("SELECT schedule_id, smart_interval_mask FROM schedules WHERE server_id = 2")
            if results_1:
                schedules_1, smart_interval_masks_1 = zip(*results_1)
            if results_2:
                schedules_2, smart_interval_masks_2 = zip(*results_2)
            snapshots_1 = query_test_db("SELECT snapshot_id FROM snapshots WHERE server_id = 1")
            snapshots_2 = query_test_db("SELECT snapshot_id FROM snapshots WHERE server_id = 2")
            assert len(schedules_1) == 5 and len(schedules_2) == 5
            assert len(snapshots_1) == 1 and len(snapshots_2) == 1
            assert query_test_db("SELECT count(*) FROM schedule_backups WHERE server_id = 1")[0][0] == 5
            assert query_test_db("SELECT count(*) FROM schedule_backups WHERE server_id = 2")[0][0] == 5
            assert all(mask is not None for mask in smart_interval_masks_1)
            assert all(mask is not None for mask in smart_interval_masks_2)
            
            assert query_test_db("""SELECT count(*) FROM schedules 
                                 LEFT JOIN schedule_backups ON schedules.schedule_id = schedule_backups.schedule_id 
                                 WHERE schedules.interval_mask != schedule_backups.interval_mask
                                 OR schedules.smart_interval_mask != schedule_backups.smart_interval_mask""")[0][0]  == 0
        finally:
            db_cur.close()
            db_conn.close()

    def test_optimise_invalid_server_id_with_custom_connection(self, db_setup):
        """Test optimise() raises error for invalid server_id with custom db connection"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Attempt to optimize for non-existent server
            with pytest.raises(ValueError, match="Server with server_id=999 does not exist"):
                smart_schedule.optimise(db_cur=db_cur, server_id=999, ga_config=None)
        finally:
            db_cur.close()
            db_conn.close()

    def test_optimise_no_schedules_with_custom_connection(self, db_setup, capsys):
        """Test optimise() handles server with no schedules gracefully using custom db connection"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create server with no schedules
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )

            # Call optimise - should return early with message
            smart_schedule.optimise(db_cur=db_cur, server_id=1, ga_config=None)

            # Verify error message was printed
            captured = capsys.readouterr()
            assert "No schedules found for server_id 1" in captured.out
        finally:
            db_cur.close()
            db_conn.close()


class TestUpdateScheduleDetailsBulk:
    """Tests for update_schedule_details_bulk function"""

    def test_update_schedule_details_bulk_single_schedule(self, db_setup):
        """Test bulk update of a single schedule"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create server and schedule
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('sched-1', 1, '0 * * * *', NULL, 'echo test')"""
            )

            # Bulk update: set smart_interval_mask
            schedule_list = [
                {
                    "schedule_id": "sched-1",
                    "smart_interval_mask": "30 * * * *",
                }
            ]
            scheduler.update_schedule_details_bulk(db_cur=db_cur, schedule_list=schedule_list)

            # Verify update
            result = query_test_db("SELECT smart_interval_mask FROM schedules WHERE schedule_id = 'sched-1'")
            assert result[0][0] == "30 * * * *"
        finally:
            db_cur.close()
            db_conn.close()

    def test_update_schedule_details_bulk_multiple_schedules(self, db_setup):
        """Test bulk update of multiple schedules"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create server and schedules
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('sched-1', 1, '0 * * * *', NULL, 'echo test1'),
                          ('sched-2', 1, '15 * * * *', NULL, 'echo test2'),
                          ('sched-3', 1, '30 * * * *', NULL, 'echo test3')"""
            )

            # Bulk update: set smart_interval_mask for all schedules
            schedule_list = [
                {"schedule_id": "sched-1", "smart_interval_mask": "10 * * * *"},
                {"schedule_id": "sched-2", "smart_interval_mask": "25 * * * *"},
                {"schedule_id": "sched-3", "smart_interval_mask": "40 * * * *"},
            ]
            scheduler.update_schedule_details_bulk(db_cur=db_cur, schedule_list=schedule_list)

            # Verify updates
            result = query_test_db(
                "SELECT schedule_id, smart_interval_mask FROM schedules WHERE server_id = 1 ORDER BY schedule_id"
            )
            assert len(result) == 3
            assert result[0] == ("sched-1", "10 * * * *")
            assert result[1] == ("sched-2", "25 * * * *")
            assert result[2] == ("sched-3", "40 * * * *")
        finally:
            db_cur.close()
            db_conn.close()

    def test_update_schedule_details_bulk_with_null_values(self, db_setup):
        """Test that NULL values in schedule_list are skipped"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create server and schedule
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('sched-1', 1, '0 * * * *', '30 * * * *', 'echo test')"""
            )

            # Bulk update: set smart_interval_mask to something new, but include None values
            schedule_list = [
                {
                    "schedule_id": "sched-1",
                    "smart_interval_mask": "45 * * * *",
                    "parameters": None,  # This should be ignored
                }
            ]
            scheduler.update_schedule_details_bulk(db_cur=db_cur, schedule_list=schedule_list)

            # Verify that only smart_interval_mask was updated
            result = query_test_db("SELECT smart_interval_mask FROM schedules WHERE schedule_id = 'sched-1'")
            assert result[0][0] == "45 * * * *"
        finally:
            db_cur.close()
            db_conn.close()

    def test_update_schedule_details_bulk_empty_list(self, db_setup):
        """Test bulk update with empty schedule list"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Should return early without error
            scheduler.update_schedule_details_bulk(db_cur=db_cur, schedule_list=[])
            # No assertion needed - test passes if no exception is raised
        finally:
            db_cur.close()
            db_conn.close()


    def test_update_schedule_details_bulk_multiple_fields(self, db_setup):
        """Test bulk update of multiple fields for each schedule"""
        db_conn, db_cur = get_db_cursor()
        try:
            # Setup: Create server and schedule
            query_test_db(
                """INSERT INTO servers (server_id, hostname, fqdn, ip4_address)
                   VALUES (1, 'test-server', 'test-server.local', '192.168.1.1')"""
            )
            query_test_db(
                """INSERT INTO schedules (schedule_id, server_id, interval_mask, smart_interval_mask, exec_command)
                   VALUES ('sched-1', 1, '0 * * * *', NULL, 'echo test'), ('sched-2', 1, '15 * * * *', NULL, 'echo test')"""
            )

            # Bulk update: update multiple fields
            schedule_list = [
                {
                    "schedule_id": "sched-1",
                    "smart_interval_mask": "15 * * * *",
                    "interval_mask": "15 * * * *",
                },
                {
                    "schedule_id": "sched-2",
                    "smart_interval_mask": "5 * * * *",
                    "interval_mask": "5 * * * *"
                }
            ]
            scheduler.update_schedule_details_bulk(db_cur=db_cur, schedule_list=schedule_list)

            # Verify updates
            result = query_test_db(
                "SELECT smart_interval_mask, interval_mask FROM schedules WHERE schedule_id = 'sched-1'"
            )
            assert result[0][0] == "15 * * * *"
            assert result[0][1] == "15 * * * *"
            result = query_test_db(
                "SELECT smart_interval_mask, interval_mask, is_enabled FROM schedules WHERE schedule_id = 'sched-2'"
            )
            assert result[0][0] == "5 * * * *"
            assert result[0][1] == "5 * * * *"
        finally:
            db_cur.close()
            db_conn.close()