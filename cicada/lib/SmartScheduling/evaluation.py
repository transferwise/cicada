import numpy as np 
from typing import Sequence 
from .domain import Schedule


def evaluate_usage_and_peak(start_times: Sequence[int], schedules: Sequence[Schedule]):
    """ 
    Returns the usage time series and peak usage for a given schedule solution 
    Args: 
        start_times: Sequence[int] : start time in minutes for each schedule
        schedules: Sequence[Schedule] : list of Schedule objects
    Returns:
        usage: np.ndarray : usage time series
        peak: float : peak usage
    """

    mins_per_day = 1440
    freqs = [schedule.frequency_minutes for schedule in schedules]
    run_times = [schedule.median_runtime_minutes for schedule in schedules]


    diff = np.zeros(mins_per_day + 1, dtype=float)
    assert len(start_times) == len(schedules) == len(freqs) == len(run_times), "Length of start_times, schedules, freqs, and run_times must all be the same"
    assert all(start_times[i] < freqs[i] for i in range(len(start_times))), "Start time should be the earliest it can be"

    for i, schedule in enumerate(schedules):
        freq = freqs[i]
        run_time = run_times[i]
        minute = int(start_times[i])

        # Iterate through the day in increments of the schedule's frequency, adding the schedule's usage to the diff array for the duration of its runtime. 
        # We use a diff array to efficiently calculate the cumulative usage at each minute. Instead of appending the usage for each minute the 
        # schedule runs in, we add the usage at the starting minute and subtract it at the end minute.
        while minute < mins_per_day:
            end = min(minute + run_time, mins_per_day)
            diff[minute] += 1
            diff[end] -= 1
            minute += freq

    # Sums up everything in the diff array to get the total usage at each minute, and finds the peak usage. 
    # Ignore the last element of the diff array since it's just a placeholder to handle the end minute subtraction for schedules that run until the end of the day.
    usage = np.cumsum(diff[:-1])
    peak = float(np.max(usage)) if usage.size else 0.0
    return usage, peak