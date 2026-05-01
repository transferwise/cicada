from __future__ import annotations
from dataclasses import dataclass
import math
from typing import Optional, List
import numpy as np
from croniter import croniter
import datetime
from ..scheduler import get_median_run_time


@dataclass(frozen=False)
class Schedule:
    schedule_id: int
    server_id: int
    interval_mask: str 
    frequency_minutes: int
    cpu_max: float = 1
    median_runtime_minutes: int = 5
    shifted: bool = False
    start_time_mins: Optional[int] = 0
    blocklisted: bool = False
    

    def __init__(self, details, db_cur):
        self.schedule_id = details['schedule_id']
        self.server_id = details['server_id']
        self.interval_mask = details['interval_mask']
        self.determine_attributes(db_cur)
        if details['blocklisted'] is not None:
            self.blocklisted = details['blocklisted']

    def determine_attributes(self, db_cur):
        """Determine frequency and average runtime from interval_mask and scheduler module"""
        self._determine_frequency()
        self._determine_start_time_mins()
        self._get_average_runtime(db_cur)

    def _determine_frequency(self):
        """Determine frequency in minutes from interval_mask using crontier"""
        schedule = croniter(self.interval_mask)
        first_iter = schedule.get_next(datetime.datetime)
        second_iter = schedule.get_next(datetime.datetime)
        frequency = (second_iter - first_iter).total_seconds() / 60 
        self.frequency_minutes = int(frequency)

    
    def _get_average_runtime(self, db_cur):
        """Get average runtime from scheduler module"""
        self.median_runtime_minutes = math.ceil(get_median_run_time(db_cur, self.schedule_id))

    def _determine_start_time_mins(self):
        """Determine the start time in minutes from midnight from the interval_mask"""

        today = datetime.datetime.now().date()
        midnight = datetime.datetime.combine(today, datetime.time.min)

        it = croniter(self.interval_mask, midnight)
        if croniter.match(self.interval_mask, midnight):
            first_iter = midnight
            self.start_time_mins = 0
        else:
            first_iter = it.get_next(datetime.datetime)
            self.start_time_mins = first_iter.hour * 60 + first_iter.minute

    def is_blocklisted(self):
        """Determine if the Schedule is blocklisted based on schedule_id"""
        return self.blocklisted

    def frequency_is_supported(self):
        """Determine if the Schedule frequency is supported for smart scheduling"""
        if (self.frequency_minutes != 1440 and self.frequency_minutes > 60): return False
        if (self.frequency_minutes <= 1): return False
        return True

    def is_unsupported(self):
        """Determine if the Schedule is unsupported for smart scheduling based on frequency or if it's blocklisted"""
        return (not self.frequency_is_supported() or self.is_blocklisted() or not self.is_regular_schedule())

    def is_regular_schedule(self):
        """Check if the cron expression is a regular schedule that can be optimized by the GA """
        try:
            schedule = croniter(self.interval_mask)
            iters = [schedule.get_next(datetime.datetime) for _ in range(20)]
            freqs = [iters[i + 1] - iters[i] for i in range(len(iters) - 1)]
            return all(f == freqs[0] for f in freqs)
        except (ValueError, KeyError):
            return False
