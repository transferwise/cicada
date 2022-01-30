#!/usr/local/bin/python3

import subprocess
import time


def main():
    child_process = subprocess.Popen(["sleep", "600"])

    while child_process.poll() is None:
        time.sleep(0.1)


if __name__ == "__main__":
    main()
