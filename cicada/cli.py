"""Cicada agent CLI."""

import argparse
import sys
import inspect

from cicada.lib import utils

from cicada.commands import register_server
from cicada.commands import list_server_schedules
from cicada.commands import exec_server_schedules
from cicada.commands import upsert_schedule
from cicada.commands import show_schedule
from cicada.commands import exec_schedule
from cicada.commands import spread_schedules
from cicada.commands import archive_schedule_log
from cicada.commands import ping_slack


@utils.named_exception_handler("Cicada")
class Cicada:
    """Cicada agent CLI."""

    def __init__(self):
        command_list = [
            "register_server",
            "list_server_schedules",
            "exec_server_schedules",
            "show_schedule",
            "upsert_schedule",
            "exec_schedule",
            "spread_schedules",
            "archive_schedule_log",
            "ping_slack",
        ]

        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog="cicada",
        )
        parser.add_argument("command", type=str, help=" ,\t".join(command_list))
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print(f"{args.command} is not a recognized command\n")
            parser.print_help()
            parser.exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)()

    @staticmethod
    def register_server():
        """Register this server"""
        register_server.main()

    @staticmethod
    def list_server_schedules():
        """List all scheduled schedules for this server"""
        list_server_schedules.main()

    @staticmethod
    def exec_server_schedules():
        """Execute all scheduled schedules for this server"""
        exec_server_schedules.main()

    @staticmethod
    def show_schedule():
        """List a schedule using schedule_id"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="List a schedule using schedule_id",
        )
        parser.add_argument(
            "--schedule_id", type=str, required=True, help="Id of the schedule"
        )
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        show_schedule.main(args.schedule_id)

    @staticmethod
    def upsert_schedule():
        """Upsert a schedule using schedule_id"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Upsert a schedule using schedule_id",
        )
        parser.add_argument(
            "--schedule_id", type=str, required=True, help="Id of the schedule"
        )
        parser.add_argument(
            "--schedule_description", type=str, help="Schedule description and comments"
        )
        parser.add_argument(
            "--server_id", type=int, help="Id of the server where the schedule will run"
        )
        parser.add_argument(
            "--schedule_order",
            type=int,
            help="run order for the schedule. lowest is first. is_async jobs will be execute in parallel",
        )
        parser.add_argument(
            "--is_async",
            type=str,
            help="0=disabled 1=Enabled | is_async jobs execute in parallel",
        )
        parser.add_argument("--is_enabled", type=str, help="0=Disabled 1=Enabled")
        parser.add_argument(
            "--adhoc_execute",
            type=str,
            help="0=Disabled 1=Enabled | Execute at next minute, regardless of other settings",
        )
        parser.add_argument(
            "--abort_running",
            type=str,
            help="0=Disabled 1=Enabled | If the job is running, it will be terminated as soon as possible",
        )
        parser.add_argument(
            "--interval_mask",
            type=str,
            help="When to execute the command | unix crontab (minute hour dom month dow)",
        )
        parser.add_argument(
            "--first_run_date",
            type=str,
            help="The schedule will not execute before this datetime",
        )
        parser.add_argument(
            "--last_run_date",
            type=str,
            help="The schedule will not execute after this datetime",
        )
        parser.add_argument("--exec_command", type=str, help="Command to execute")
        parser.add_argument(
            "--parameters", type=str, help="Parameters for exec_command"
        )
        parser.add_argument(
            "--adhoc_parameters",
            type=str,
            help="If specified, will override parameters for one run",
        )
        parser.add_argument(
            "--schedule_group_id",
            type=int,
            help="Optional field to help with schedule grouping",
        )
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        upsert_schedule.main(vars(args))

    @staticmethod
    def exec_schedule():
        """Execute a using schedule_id"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Execute a using schedule_id",
        )
        parser.add_argument(
            "--schedule_id", type=str, required=True, help="Id of the schedule"
        )
        # now that we're inside a subcommand, ignore the first TWO args
        # Also only accept ONE unknown arg
        args, uargs = parser.parse_known_args(sys.argv[2:4])
        if len(uargs) == 1:
            dbname = uargs[0]
        else:
            dbname = None
        exec_schedule.main(args.schedule_id, dbname)

    @staticmethod
    def spread_schedules():
        """Spread schedules accross servers"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Spread schedules accross servers",
        )
        parser.add_argument(
            "--from_server_ids",
            type=str,
            required=True,
            help="List of source server_ids to collect schedules from",
        )
        parser.add_argument(
            "--to_server_ids",
            type=str,
            required=True,
            help="List of target server_ids to spread schedules to",
        )
        parser.add_argument(
            "--commit",
            default=False,
            action="store_true",
            help="Commits changes to backend DB, otherwise only print output",
        )
        parser.add_argument(
            "--force",
            default=False,
            action="store_true",
            help="If schedule is moving servers and also currently running, perform abort_running and adhoc_execute "
            "| Only available when --commit is specified",
        )
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        spread_schedules.main(vars(args))

    @staticmethod
    def archive_schedule_log():
        """Archive entries from schedule_log into schedule_log_historical"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Archive entries from schedule_log into schedule_log_historical",
        )
        parser.add_argument(
            "--days_to_keep",
            type=int,
            required=True,
            help="Amount of days to keep in schedule_log",
        )
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        archive_schedule_log.main(args.days_to_keep)

    @staticmethod
    def ping_slack():
        """Send a test message to Slack"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Send a test message to Slack",
        )
        parser.add_argument(
            "--text", type=str, required=True, help="Text to send to Slack"
        )
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        ping_slack.main(args.text)


def main():
    """Cicada agent CLI."""
    Cicada()
