#!/usr/bin/python

import os
import sys
import socket
import datetime

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../../lib"))
import libEmail


def main():
    # Check parameters
    if len(sys.argv) < 2:
        sys.stderr.write("Script usage: " + sys.argv[0] + " example1@server.domain example2@server.domain\n")
        exit(101)

    # If parameters are ok, run script
    msgTo = sys.argv
    del msgTo[0]

    hostname = str(socket.gethostname())
    now = str(datetime.datetime.now())

    msgSubject = "Test Mail sent from " + hostname + " at " + now
    msgBody = "Test Mail sent\nfrom " + hostname + "\nat " + now

    libEmail.sendEmail(msgSubject, msgBody, msgTo)


if __name__ == "__main__":
    main()
