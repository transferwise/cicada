import numpy as np 
from typing import Sequence 
from .domain import Tap


def evaluate_cpu_usage_and_peak(start_times: Sequence[int], taps: Sequence[Tap]):
    """ 
    Returns the CPU usage time series and peak CPU usage for a given schedule solution 
    Args: 
        start_times: Sequence[int] : start time in minutes for each tap
        taps: Sequence[Tap] : list of Tap objects
    Returns:
        usage: np.ndarray : CPU usage time series
        peak: float : peak CPU usage
    """

    mins_per_day = 1440
    freqs = [tap.frequency_minutes for tap in taps]
    run_times = [tap.median_runtime_minutes for tap in taps]


    diff = np.zeros(mins_per_day + 1, dtype=float)
    assert len(start_times) == len(taps) == len(freqs) == len(run_times), "Length of start_times, taps, freqs, and run_times must all be the same"
    assert all(start_times[i] < freqs[i] for i in range(len(start_times))), "Start time should be the earliest it can be"

    for i, tap in enumerate(taps):
        freq = freqs[i]
        run_time = run_times[i]
        cpu = float(tap.cpu_max)
        minute = int(start_times[i])

        # Iterate through the day in increments of the tap's frequency, adding the tap's CPU usage to the diff array for the duration of its runtime. 
        # We use a diff array to efficiently calculate the cumulative CPU usage at each minute. Instead of appending the CPU usage for each minute the 
        # tap runs in, we add the CPU usage at the starting minute and subtract it at the end minute.
        while minute < mins_per_day:
            end = min(minute + run_time, mins_per_day)
            diff[minute] += cpu
            diff[end] -= cpu
            minute += freq

    # Sums up everything in the diff array to get the total CPU usage at each minute, and finds the peak usage. 
    # Ignore the last element of the diff array since it's just a placeholder to handle the end minute subtraction for taps that run until the end of the day.
    usage = np.cumsum(diff[:-1])
    peak = float(np.max(usage)) if usage.size else 0.0
    return usage, peak