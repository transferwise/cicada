from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import numpy as np
from croniter import croniter
import datetime
from ..scheduler import get_median_run_time


@dataclass(frozen=False)
class Tap:
    schedule_id: int
    server_id: int
    interval_mask: str 
    frequency_minutes: int
    cpu_max: float = 1
    median_runtime_minutes: int = 5
    shift: Optional[int] = 0
    start_time_mins: Optional[int] = None
    

    def __init__(self, details, db_cur):
        self.schedule_id = details['schedule_id']
        self.server_id = details['server_id']
        self.interval_mask = details['interval_mask']
        self.determine_attributes(db_cur)

    def determine_attributes(self, db_cur):
        """Determine frequency and average runtime from interval_mask and scheduler module"""
        self._determine_frequency()
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
        # for local testing set everything to 5 mins
        self.median_runtime_minutes = 5
        # self.median_runtime_minutes = get_median_run_time(db_cur, self.schedule_id)

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

    def is_blacklisted(self):
        """Determine if the tap is blacklisted based on schedule_id"""
        return False 
        # Change implementation to check against blacklist in DB once blacklist functionality is implemented
        # Blacklist shouldn't be stored in GA and instead be in db
        return self.schedule_id in self.cfg.blacklist_schedule_ids

    def is_unsupported(self):
        """Determine if the tap is unsupported for smart scheduling based on frequency or if it's blacklisted"""
        return ((self.frequency_minutes != 1440 and self.frequency_minutes > 60) or self.is_blacklisted() or not self.is_regular_schedule())

    def is_regular_schedule(self):
        """Check if the cron expression is a regular schedule that can be optimized by the GA """
        try:
            schedule = croniter(self.interval_mask)
            iter1 = schedule.get_next(datetime.datetime)
            iter2 = schedule.get_next(datetime.datetime)
            iter3 = schedule.get_next(datetime.datetime)
            iter4 = schedule.get_next(datetime.datetime)
            iter5 = schedule.get_next(datetime.datetime)
            freq1 = (iter2 - iter1)
            freq2 = (iter3 - iter2)
            freq3 = (iter4 - iter3)
            freq4 = (iter5 - iter4)
            return freq1 == freq2 == freq3 == freq4
        except (ValueError, KeyError):
            return False
