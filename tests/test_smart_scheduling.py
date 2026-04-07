"""Tests for smart scheduling and rollback functionality"""

import croniter
import pytest
from unittest.mock import Mock, MagicMock, patch, call
import numpy as np
from datetime import datetime, timedelta

from cicada.lib.SmartScheduling.domain import Tap
from cicada.lib.SmartScheduling.config import GAConfig
from cicada.lib.SmartScheduling.evaluation import (
    calculate_blocks_per_day,
    discretize_taps,
    evaluate_cpu_usage_and_peak,
)
import cicada.commands.smart_schedule as smart_schedule
from cicada.lib.SmartScheduling.pygad import GAPyGADScheduler
from cicada.lib import scheduler


class TestCalculateBlocksPerDay:
    """Tests for calculate_blocks_per_day function"""

    def test_calculate_blocks_per_day_1_minute_blocks(self):
        """Test calculating blocks per day with 1-minute blocks"""
        blocks = calculate_blocks_per_day(1)
        assert blocks == 1440

    def test_calculate_blocks_per_day_5_minute_blocks(self):
        """Test calculating blocks per day with 5-minute blocks"""
        blocks = calculate_blocks_per_day(5)
        assert blocks == 288

    def test_calculate_blocks_per_day_60_minute_blocks(self):
        """Test calculating blocks per day with 60-minute blocks"""
        blocks = calculate_blocks_per_day(60)
        assert blocks == 24

    def test_calculate_blocks_per_day_invalid_divisor(self):
        """Test that invalid divisors raise ValueError"""
        with pytest.raises(ValueError):
            calculate_blocks_per_day(7)

    def test_calculate_blocks_per_day_1440(self):
        """Test that 1440 minutes divides evenly"""
        blocks = calculate_blocks_per_day(1440)
        assert blocks == 1

    def test_calculate_blocks_per_day_greater_than_1440(self):
        """Test that minutes_per_block greater than 1440 raises ValueError"""
        with pytest.raises(ValueError):
            calculate_blocks_per_day(1500)

    def test_calculate_blocks_per_day_zero(self):
        """Test that zero minutes per block raises ValueError"""
        with pytest.raises(ZeroDivisionError):
            calculate_blocks_per_day(0)

    def test_calculate_blocks_per_day_non_divisible(self):
        """Test that non-divisible minutes per block raises ValueError"""
        with pytest.raises(ValueError):
            calculate_blocks_per_day(7)


class TestDiscretizeTaps:
    """Tests for discretize_taps function"""

    def test_discretize_single_tap(self):
        """Test discretizing a single tap"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",  # Every hour
        }
        tap = Tap(tap_details, db_cur)
        tap.frequency_minutes = 60
        tap.median_runtime_minutes = 5

        freq_blocks, run_blocks = discretize_taps([tap], minutes_per_block=1)

        assert freq_blocks == [60]
        assert run_blocks == [5]

        freq_blocks, run_blocks = discretize_taps([tap], minutes_per_block=2)

        assert freq_blocks == [30]
        assert run_blocks == [3]


    def test_discretize_multiple_taps(self):
        """Test discretizing multiple taps with different frequencies"""
        db_cur = Mock()

        tap1_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap1 = Tap(tap1_details, db_cur)
        tap1.frequency_minutes = 60
        tap1.median_runtime_minutes = 5

        tap2_details = {
            "schedule_id": 2,
            "server_id": 1,
            "interval_mask": "*/15 * * * *",
        }
        tap2 = Tap(tap2_details, db_cur)
        tap2.frequency_minutes = 15
        tap2.median_runtime_minutes = 3

        freq_blocks, run_blocks = discretize_taps([tap1, tap2], minutes_per_block=1)

        assert freq_blocks == [60, 15]
        assert run_blocks == [5, 3]


    def test_discretize_minimum_blocks(self):
        """Test that minimum block size is 1"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap = Tap(tap_details, db_cur)
        tap.frequency_minutes = 2
        tap.median_runtime_minutes = 1

        freq_blocks, run_blocks = discretize_taps([tap], minutes_per_block=5)

        assert freq_blocks == [1]
        assert run_blocks == [1]


class TestEvaluateCpuUsageAndPeak:
    """Tests for evaluate_cpu_usage_and_peak function"""

    def test_evaluate_single_tap_no_overlap(self):
        """Test CPU evaluation with a single tap that doesn't overlap"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap = Tap(tap_details, db_cur)
        tap.frequency_minutes = 60
        tap.median_runtime_minutes = 5
        tap.cpu_max = 1

        start_blocks = [0]
        usage, peak = evaluate_cpu_usage_and_peak(start_blocks, [tap], minutes_per_block=1)

        assert usage.shape == (1440,)
        assert peak == 1
        for i in range(24):
            mins = i * 60
            assert (usage[mins:mins+5] == 1).all()
            assert (usage[mins+5:(i+1)*60] == 0).all()

    def test_evaluate_multiple_taps_no_overlap(self):
        """Test CPU evaluation with multiple taps that don't overlap"""
        db_cur = Mock()

        tap1_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap1 = Tap(tap1_details, db_cur)
        tap1.frequency_minutes = 60
        tap1.median_runtime_minutes = 5
        tap1.cpu_max = 0.5

        tap2_details = {
            "schedule_id": 2,
            "server_id": 1,
            "interval_mask": "30 * * * *",
        }
        tap2 = Tap(tap2_details, db_cur)
        tap2.frequency_minutes = 60
        tap2.median_runtime_minutes = 5
        tap2.cpu_max = 0.3

        start_blocks = [0, 30]
        usage, peak = evaluate_cpu_usage_and_peak(start_blocks, [tap1, tap2], minutes_per_block=1)

        assert peak == 0.5  
        assert (usage[0:5] == 0.5).all()
        assert (usage[6:30] == 0.0).all()
        assert (usage[30:35] == 0.3).all()
        assert (usage[35:60] == 0.0).all()

    def test_evaluate_overlapping_taps(self):
        """Test CPU evaluation with overlapping taps"""
        db_cur = Mock()

        tap1_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap1 = Tap(tap1_details, db_cur)
        tap1.frequency_minutes = 60
        tap1.median_runtime_minutes = 10
        tap1.cpu_max = 0.5

        tap2_details = {
            "schedule_id": 2,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap2 = Tap(tap2_details, db_cur)
        tap2.frequency_minutes = 60
        tap2.median_runtime_minutes = 5
        tap2.cpu_max = 0.3

        start_blocks = [0, 0]
        usage, peak = evaluate_cpu_usage_and_peak(start_blocks, [tap1, tap2], minutes_per_block=1)

        assert peak == 0.8
        assert usage[0] == 0.8
        assert usage[5] == 0.5 


    def test_evaluate_wrapping_around_day(self):
        """Test that taps wrapping around midnight work correctly"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": 1,
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap = Tap(tap_details, db_cur)
        tap.frequency_minutes = 60
        tap.median_runtime_minutes = 5
        tap.cpu_max = 1.0
        start_blocks = [1430]  # (1430 mins = 23:50)

        # Should throw an assertion error that the start block is too late for the frequency of the tap
        with pytest.raises(AssertionError):
            evaluate_cpu_usage_and_peak(start_blocks, [tap], minutes_per_block=1)


class TestTapDomain:
    """Tests for Tap domain object"""

    def test_tap_initialization(self):
        """Test Tap object initialization"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 5,
            "interval_mask": "0 * * * *",
        }
        tap = Tap(tap_details, db_cur)

        assert tap.schedule_id == "test-id-1"
        assert tap.server_id == 5
        assert tap.interval_mask == "0 * * * *"

    def test_tap_frequency_hourly(self):
        """Test frequency determination for hourly cron"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0 * * * *",  # Every hour
        }
        tap = Tap(tap_details, db_cur)

        assert tap.frequency_minutes == 60

    def test_tap_frequency_daily(self):
        """Test frequency determination for daily cron"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0 0 * * *",
        }
        tap = Tap(tap_details, db_cur)

        assert tap.frequency_minutes == 1440

    def test_tap_is_unsupported_irregular_cron(self):
        """Test that taps with irregular cron expressions are marked as unsupported"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0-15 */9 * * *",  
        }
        tap = Tap(tap_details, db_cur)

        assert tap.is_unsupported()
        assert not tap.frequency_is_supported()
        assert not tap.is_regular_schedule()

    def test_tap_is_unsupported_low_frequency(self):
        """Test that taps with unsupported low frequencies are marked as unsupported"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0 0 * * 0",  # Weekly
        }
        tap = Tap(tap_details, db_cur)

        assert tap.is_unsupported()

    def test_tap_is_regular_schedule_hourly(self):
        """Test that hourly schedules are recognized as regular"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap = Tap(tap_details, db_cur)

        assert tap.is_regular_schedule()

    def test_tap_is_regular_schedule_every_15_mins(self):
        """Test that every-15-minute schedules are recognized as regular"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "*/15 * * * *",
        }
        tap = Tap(tap_details, db_cur)

        assert tap.is_regular_schedule()

    def test_tap_is_regular_schedule_daily(self):
        """Test that daily schedules are recognized as regular"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0 0 * * *",
        }
        tap = Tap(tap_details, db_cur)

        assert tap.is_regular_schedule()

    def test_tap_45_min_schedule_is_supported(self):
        """Test that 45-minute frequency schedules are recognized as supported"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "*/45 * * * *",
        }
        tap = Tap(tap_details, db_cur)

        assert not tap.is_unsupported() 
        # Fails due to cronitor issue -> means any */45 gets missed out of the smart scheduling

    def test_tap_is_irregular_schedule_weekdays(self):
        """Test that weekday-only schedules are marked as irregular"""
        db_cur = Mock()
        tap_details = {
            "schedule_id": "test-id-1",
            "server_id": 1,
            "interval_mask": "0 9 * * 1-5",  # Weekdays only
        }
        tap = Tap(tap_details, db_cur)

        assert not tap.is_regular_schedule()


class TestGAConfig:
    """Tests for GAConfig configuration class"""

    def test_custom_config(self):
        """Test GAConfig with custom values"""
        config = GAConfig(
            minutes_per_block=5,
            num_generations=50,
            sol_per_pop=100,
            random_seed=42,
        )
        assert config.minutes_per_block == 5
        assert config.num_generations == 50
        assert config.sol_per_pop == 100
        assert config.random_seed == 42
        assert config.num_parents_mating == 10
        assert config.mutation_percent_genes == 20
        assert config.parent_selection_type == "rank"
        assert config.crossover_type == "uniform"
        assert config.mutation_type == "random"
        assert config.keep_elitism == 1
        assert config.blacklist_schedule_ids == []


class TestGAPyGADScheduler:
    """Tests for GAPyGADScheduler"""

    def test_scheduler_uses_default_config_when_optional_config_is_missing(self):
        scheduler = GAPyGADScheduler()

        assert scheduler.cfg == GAConfig()
        assert scheduler.cfg.minutes_per_block == 1
        assert scheduler.cfg.num_generations == 20

    def test_scheduler_initialization_custom_config(self):
        """Test scheduler initialization with custom config"""
        config = {"minutes_per_block": 5, "num_generations": 30}
        scheduler = GAPyGADScheduler(config)

        assert scheduler.cfg.minutes_per_block == 5
        assert scheduler.cfg.num_generations == 30

    def test_scheduler_initialization_filters_none_values(self):
        """Test that None values are filtered out when initializing config"""
        config = {"minutes_per_block": 5, "num_generations": None}
        scheduler = GAPyGADScheduler(config)

        assert scheduler.cfg.minutes_per_block == 5
        assert scheduler.cfg.num_generations == 20


class TestSchedulerDatabaseFunctions:
    """Tests for scheduler database functions (rollback/backup)"""

    def test_get_all_schedule_backups(self):
        """Test retrieving all schedule backups"""
        db_cur = Mock()
        db_cur.fetchall.return_value = [
            ("schedule-1", 10, "0 * * * *"),
            ("schedule-2", 20, "0 0 * * *"),
        ]

        result = scheduler.get_all_schedule_backups(db_cur)

        assert len(result) == 2
        assert result[0] == ("schedule-1", 10, "0 * * * *")
        assert result[1] == ("schedule-2", 20, "0 0 * * *")
        db_cur.execute.assert_called_once()

    def test_restore_previous_schedules_requires_scope(self):
        """Test that restore_previous_schedules requires either schedule_id or server_id"""
        db_cur = Mock()

        with pytest.raises(ValueError):
            scheduler.restore_previous_schedules(db_cur)


class TestEndToEndSmartScheduling:
    """Integration tests for end-to-end smart scheduling workflow"""

    def test_create_taps_from_details(self):
        """Test creating multiple Tap objects from details"""
        db_cur = Mock()

        taps_data = [
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

        taps = [Tap(data, db_cur) for data in taps_data]

        assert len(taps) == 2
        assert taps[0].schedule_id == "sched-1"
        assert taps[1].schedule_id == "sched-2"

    def test_discretize_and_evaluate_flow(self):
        """Test the flow of discretizing taps and evaluating CPU"""
        db_cur = Mock()

        tap_data = {
            "schedule_id": "sched-1",
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        tap = Tap(tap_data, db_cur)
        tap.frequency_minutes = 60
        tap.median_runtime_minutes = 5
        tap.cpu_max = 0.5

        # Discretize
        freq_blocks, run_blocks = discretize_taps([tap], minutes_per_block=1)
        assert len(freq_blocks) == 1
        assert len(run_blocks) == 1

        # Evaluate
        start_blocks = [0]
        usage, peak = evaluate_cpu_usage_and_peak(start_blocks, [tap], minutes_per_block=1)
        assert peak == 0.5
        assert usage.shape == (1440,)

    def test_backup_restore_workflow(self):
        """Test the workflow of backing up and restoring schedules"""
        db_cur = Mock()

        # Step 1: Update backups
        schedule_details = {
            "schedule_id": "sched-1",
            "server_id": 1,
            "interval_mask": "0 * * * *",
            "previous_interval_mask": "30 * * * *",
            "start_time_shift_mins": 30
        }
        scheduler.update_schedule_backups(db_cur, schedule_details)
        assert db_cur.execute.call_count == 1

        # Step 2: Restore schedules
        db_cur.reset_mock()
        scheduler.restore_previous_schedules(db_cur, schedule_id="sched-1")
        assert db_cur.execute.call_count >= 1

    def test_multiple_overlapping_taps_evaluation(self):
        """Test evaluating CPU usage for multiple overlapping taps"""
        db_cur = Mock()

        # Create 3 taps with different schedules
        taps = []
        for i in range(3):
            tap_data = {
                "schedule_id": f"sched-{i}",
                "server_id": 1,
                "interval_mask": "0 * * * *" if i == 0 else f"*/{15 * (i + 1)} * * * *",
            }
            tap = Tap(tap_data, db_cur)
            tap.frequency_minutes = 60
            tap.median_runtime_minutes = 5
            tap.cpu_max = 0.3 + (i * 0.2)
            taps.append(tap)

        # Stagger start times to create overlaps
        start_blocks = [0, 10, 20]
        usage, peak = evaluate_cpu_usage_and_peak(start_blocks, taps, minutes_per_block=1)

        assert peak > 0.3  # Should have some overlapping usage
        assert usage.shape == (1440,)

class TestSmartSchedulingCommand:
    """Tests for the smart scheduling command"""

    def test_smart_scheduling_frequency_unchanged_hourly_tap(self):
        """Test that the frequency of the schedule remains unchanged after smart scheduling"""
        db_cur = Mock()

        hourly_tap_details = {
            "schedule_id": "test-schedule-1",
            "server_id": 1,
            "interval_mask": "0 * * * *",
        }
        hourly_tap = Tap(hourly_tap_details, db_cur)
        hourly_tap.shift = 15
        
        smart_schedule.update_schedule_cron(hourly_tap)
        assert hourly_tap.interval_mask == "15 * * * *"
        assert hourly_tap.frequency_minutes == 60

        hourly_tap.determine_attributes(db_cur)
        assert hourly_tap.is_regular_schedule()
        assert hourly_tap.frequency_minutes == 60

        hourly_tap.interval_mask = "*/60 * * * *"
        smart_schedule.update_schedule_cron(hourly_tap)
        assert hourly_tap.interval_mask == "15 * * * *"
        assert hourly_tap.frequency_minutes == 60


    def test_smart_scheduling_frequency_unchanged_fifteen_min_tap(self):
        """Test that the frequency of the schedule remains unchanged after smart scheduling"""
        db_cur = Mock()

        fifteen_min_tap_details = {
            "schedule_id": "test-schedule-2",
            "server_id": 1,
            "interval_mask": "*/15 * * * *",
        }
        fifteen_min_tap = Tap(fifteen_min_tap_details, db_cur)
        fifteen_min_tap.shift = 3
        
        smart_schedule.update_schedule_cron(fifteen_min_tap)
        assert fifteen_min_tap.interval_mask == "3-59/15 * * * *"
        assert fifteen_min_tap.frequency_minutes == 15
        
        fifteen_min_tap.determine_attributes(db_cur)
        assert fifteen_min_tap.is_regular_schedule()
        assert fifteen_min_tap.frequency_minutes == 15

        
    def test_gene_space_constraints(self):
        """Test that the gene space constraints are respected when updating schedule crons"""
        db_cur = Mock()

        tap_details = {
            "schedule_id": "test-schedule-3",
            "server_id": 1,
            "interval_mask": "*/45 * * * *",
        }
        tap = Tap(tap_details, db_cur)
        tap.frequency_minutes = 45
        tap.shift = 50  # Shift greater than frequency

        with pytest.raises(AssertionError):
            smart_schedule.update_schedule_cron(tap)

    def test_smart_scheduling_gene_space_constraints_30_min(self):
        """Test that the gene space constraints don't create invalid cron expressions"""
        db_cur = Mock()
        smartScheduler = GAPyGADScheduler()

        tap_details = {
            "schedule_id": "test-schedule-4",
            "server_id": 1,
            "interval_mask": "*/30 * * * *",
        }
        tap = Tap(tap_details, db_cur)
        gene_space = (smartScheduler._gene_space([tap]))

        tap.shift = gene_space[0][-1] 
        smart_schedule.update_schedule_cron(tap)
        assert tap.interval_mask == "29-59/30 * * * *"
        assert tap.is_regular_schedule()
        assert croniter.croniter.is_valid(tap.interval_mask)
        tap.determine_attributes(db_cur)
        assert tap.frequency_minutes == 30

        tap.interval_mask = "*/30 * * * *"  # Reset to original
        tap.shift = gene_space[0][0] 
        smart_schedule.update_schedule_cron(tap)
        print(tap.shift, tap.interval_mask)
        assert tap.interval_mask == "*/30 * * * *"
        assert croniter.croniter.is_valid(tap.interval_mask)
        tap.determine_attributes(db_cur)
        assert tap.frequency_minutes == 30

        tap.shift = gene_space[0][1] 
        smart_schedule.update_schedule_cron(tap)
        print(tap.shift, tap.interval_mask)
        assert tap.interval_mask == "1-59/30 * * * *"
        assert croniter.croniter.is_valid(tap.interval_mask)
        tap.determine_attributes(db_cur)
        assert tap.frequency_minutes == 30

    def test_smart_scheduling_gene_space_constraints_daily(self):
        """Test that the gene space constraints don't create invalid cron expressions"""
        db_cur = Mock()
        smartScheduler = GAPyGADScheduler()

        tap_details = {
            "schedule_id": "test-schedule-4",
            "server_id": 1,
            "interval_mask": "30 8 * * *",
        }
        tap = Tap(tap_details, db_cur)
        gene_space = (smartScheduler._gene_space([tap]))

        tap.shift = gene_space[0][-1] 
        smart_schedule.update_schedule_cron(tap)
        assert tap.interval_mask == "29 9 * * *"
        assert tap.is_regular_schedule()
        assert croniter.croniter.is_valid(tap.interval_mask)
        tap.determine_attributes(db_cur)
        assert tap.frequency_minutes == 1440

        tap.interval_mask = "30 8 * * *"  # Reset to original
        tap.shift = gene_space[0][0] 
        smart_schedule.update_schedule_cron(tap)
        print(tap.shift, tap.interval_mask)
        assert tap.interval_mask == "30 8 * * *"
        assert croniter.croniter.is_valid(tap.interval_mask)
        tap.determine_attributes(db_cur)
        assert tap.frequency_minutes == 1440

        tap.shift = gene_space[0][1] 
        smart_schedule.update_schedule_cron(tap)
        print(tap.shift, tap.interval_mask)
        assert tap.interval_mask == "31 8 * * *"
        assert croniter.croniter.is_valid(tap.interval_mask)
        tap.determine_attributes(db_cur)
        assert tap.frequency_minutes == 1440
