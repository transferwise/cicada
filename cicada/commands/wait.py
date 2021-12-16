"""Wait. Just wait."""

import datetime
import time


def main(seconds: float):
    """Wait. Just wait."""
    print(f"Waiting {str(seconds)} seconds from {str(datetime.datetime.now())}")
    time.sleep(seconds)
