#!/usr/bin/python

import sys
import datetime
import time


def main():
    # Check parameters
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: " + sys.argv[0] + " {seconds}\n")
        exit(101)

    # If parameters are ok, run script
    someSeconds = sys.argv[1]
    print("Waiting " + someSeconds + " seconds from " + str(datetime.datetime.now()))
    time.sleep(float(someSeconds))


if __name__ == "__main__":
    main()
