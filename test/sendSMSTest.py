#!/usr/bin/python

import os
import sys
import socket
import datetime

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../../lib"))
import libSMS


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Script usage: " + sys.argv[0] + " 27821234567 27831234567\n")
        exit(101)

    # If parameters are ok, run script
    msgTo = sys.argv
    del msgTo[0]

    hostname = str(socket.gethostname())
    now = str(datetime.datetime.now())
    msgText = "Test SMS sent\nfrom " + hostname + "\nat " + now

    libSMS.sendSMS(msgText, msgTo)

if __name__ == "__main__":
    main()
