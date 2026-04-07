import math 
import numpy as np 
from typing import Sequence, Tuple, List 
from .domain import Tap


def calculate_blocks_per_day(minutes_per_block: int) -> int:
    """
    Calculate the number of time blocks in a day given the minutes per block.
    Raises error if the minutes_per_block does not give a whole number of blocks per day
    Args:
        minutes_per_block: int : number of minutes per time block
    Returns:
        int : number of time blocks in a day
    """
    if (24 * 60) % minutes_per_block != 0:
        raise ValueError("minutes_per_block must divide evenly into 1440 (the number of minutes in a day)")
    return (24 * 60) // minutes_per_block


def discretize_taps(taps: Sequence[Tap], minutes_per_block: int) -> Tuple[List[int], List[int]]:
    """
    Discretize taps into frequency and runtime blocks based on minutes per block.
    Args:
        taps: Sequence[Tap] : list of Tap objects
        minutes_per_block: int : number of minutes per time block
    Returns:
        Tuple[List[int], List[int]] : frequency blocks and runtime blocks for each tap
        freq_blocks: List[int] : amount of time blocks between each run of the tap (based on frequency_minutes)
        run_blocks: List[int] : amount of time blocks the tap runs for (based on median_runtime_minutes)
    """
    freq_blocks, run_blocks = [], []
    for t in taps:
        fb = max(1, t.frequency_minutes // minutes_per_block)
        rb = max(1, math.ceil(t.median_runtime_minutes / minutes_per_block))
        freq_blocks.append(fb)
        run_blocks.append(rb)
    return freq_blocks, run_blocks

def evaluate_cpu_usage_and_peak(start_blocks: Sequence[int], taps: Sequence[Tap], minutes_per_block: int):
    """ 
    Returns the CPU usage time series and peak CPU usage for a given schedule solution 
    Args: 
        start_blocks: Sequence[int] : start time blocks for each tap
        taps: Sequence[Tap] : list of Tap objects
        minutes_per_block: int : number of minutes per time block
    Returns:
        usage: np.ndarray : CPU usage time series
        peak: float : peak CPU usage
    """

    blocks_per_day = calculate_blocks_per_day(minutes_per_block)
    freq_blocks, run_blocks = discretize_taps(taps, minutes_per_block)
    diff = np.zeros(blocks_per_day + 1, dtype=float)
    assert len(start_blocks) == len(taps) == len(freq_blocks) == len(run_blocks), "Length of start_blocks, taps, freq_blocks, and run_blocks must all be the same"
    assert all(start_blocks[i] < freq_blocks[i] for i in range(len(start_blocks))), "Start block should be the earliest it can be"

    for i, tap in enumerate(taps):
        freq = freq_blocks[i]
        run_len = run_blocks[i]
        cpu = float(tap.cpu_max)
        block = int(start_blocks[i])

        # Iterate through the day in increments of the tap's frequency, adding the tap's CPU usage to the diff array for the duration 
        # of its runtime. We use a diff array to efficiently calculate the cumulative CPU usage at each time block. Instead of 
        # appending the CPU usage for each block the tap runs in, we add the CPU usage at the start block and subtract it at the end block.
        while block < blocks_per_day:
            end = min(block + run_len, blocks_per_day)
            diff[block] += cpu
            diff[end] -= cpu
            block += freq

    # Sums up everything in the diff array to get the total CPU usage at each time block, and finds the peak usage. 
    # Ignore the last element of the diff array since it's just a placeholder to handle the end block subtraction for taps that run until the end of the day.
    usage = np.cumsum(diff[:-1])
    peak = float(np.max(usage)) if usage.size else 0.0
    return usage, peak