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
from cicada.lib.SmartScheduling.pygad import GAPyGADScheduler
from cicada.lib import scheduler


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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            schedule1 = Schedule(schedule1_details, db_cur)
            schedule1.frequency_minutes = 60
            schedule1.median_runtime_minutes = 5

            schedule2_details = {
                "schedule_id": 2,
                "server_id": 1,
                "interval_mask": "30 * * * *",
            }
            schedule2 = Schedule(schedule2_details, db_cur)
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
            }
            schedule1 = Schedule(schedule1_details, db_cur)
            schedule1.frequency_minutes = 60
            schedule1.median_runtime_minutes = 10

            schedule2_details = {
                "schedule_id": 2,
                "server_id": 1,
                "interval_mask": "0 * * * *",
            }
            schedule2 = Schedule(schedule2_details, db_cur)
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
            }
            test_schedule = Schedule(schedule_details, db_cur)
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
            }
            test_schedule = Schedule(schedule_details, db_cur)

            assert test_schedule.schedule_id == "test-id-1"
            assert test_schedule.server_id == 5
            assert test_schedule.interval_mask == "0 * * * *"
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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
            }
            test_schedule = Schedule(schedule_details, db_cur)

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
        assert config.blocklist_schedule_ids == []

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
            scheduler.snapshot_schedules(db_cur, ["test-sched-1"], reason="Test optimization")

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
                },
                {
                    "schedule_id": "sched-2",
                    "server_id": 1,
                    "interval_mask": "*/30 * * * *",
                },
            ]

            schedules = [Schedule(data, db_cur) for data in schedules_data]

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

            schedule_ids = ["sched-1", "sched-2"]
            scheduler.snapshot_schedules(db_cur, schedule_ids, reason="Test optimization")

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
                }
                test_schedule = Schedule(schedule_data, db_cur)
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
            }
            hourly_schedule = Schedule(hourly_schedule_details, db_cur)
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
            }
            fifteen_min_schedule = Schedule(fifteen_min_schedule_details, db_cur)
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
            }
            test_schedule = Schedule(schedule_details, db_cur)
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
                "schedule_id": "test-schedule-4",
                "server_id": 1,
                "interval_mask": "*/30 * * * *",
            }
            test_schedule = Schedule(schedule_details, db_cur)
            gene_space = ga_scheduler._gene_space([test_schedule])

            test_schedule.shifted = True
            test_schedule.start_time_mins = gene_space[0][-1]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "29-59/30 * * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 30

            test_schedule.start_time_mins = gene_space[0][1]
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
            }
            test_schedule = Schedule(schedule_details, db_cur)
            gene_space = ga_scheduler._gene_space([test_schedule])

            test_schedule.shifted = True
            test_schedule.start_time_mins = gene_space[0][-1]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "29 9 * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 1440

            test_schedule.start_time_mins = gene_space[0][0]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "30 8 * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 1440

            test_schedule.start_time_mins = gene_space[0][1]
            smart_schedule._update_schedule_cron(test_schedule)
            assert test_schedule.smart_interval_mask == "31 8 * * *"
            assert croniter.croniter.is_valid(test_schedule.smart_interval_mask)
            assert test_schedule.frequency_minutes == 1440
        finally:
            db_cur.close()
            db_conn.close()


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
            scheduler.snapshot_schedules(db_cur, schedule_ids, server_id = 1, reason="Test optimization")

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
            scheduler.snapshot_schedules(db_cur, ["test-schedule-1"], reason="Test optimization", server_id=1)
            assert query_test_db("SELECT smart_interval_mask FROM schedule_backups WHERE schedule_id = 'test-schedule-1'")[0][0] == "30 * * * *"
            assert query_test_db("SELECT COUNT(*) FROM schedule_backups WHERE schedule_id = 'test-schedule-1'")[0][0] == 1

            query_test_db("UPDATE schedules SET smart_interval_mask = '45 * * * *' WHERE schedule_id = 'test-schedule-1'")

            scheduler.snapshot_schedules(db_cur, ["test-schedule-1"], reason="Test optimization", server_id=1)
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