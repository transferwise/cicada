#!/usr/bin/python

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))
import utils


def main():
    parser = argparse.ArgumentParser(description='Send a test message to configured Slack channel', add_help=True)
    parser.add_argument('--text', type=str, nargs='?', const='Some more text',
            help='Expanded text to send to Slack channel')
    args = parser.parse_args()

    if args.text is None:
        text = 'Default text'
    else:
        text = args.text

    utils.send_slack_message(':dart:  Message from cicada *_pingSlack.py_*', text, 'good')


if __name__ == '__main__':
    main()
