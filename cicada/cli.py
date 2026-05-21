"""Cicada agent CLI."""

import argparse
import sys
import inspect
from pkg_resources import get_distribution

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
from cicada.commands import list_schedules
from cicada.commands import delete_schedule
from cicada.commands import smart_schedule
from cicada.commands import blocklist_schedule as blocklist_schedule_cmd


@utils.named_exception_handler("Cicada")
class Cicada:
    """Cicada agent CLI."""

    def __init__(self):
        command_list = [
            "register_server",
            "list_server_schedules",
            "exec_server_schedules",
            "smart_schedule",
            "show_schedule",
            "upsert_schedule",
            "exec_schedule",
            "spread_schedules",
            "archive_schedule_log",
            "ping_slack",
            "list_schedule_ids",
            "delete_schedule",
            "version",
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
        parser.add_argument("--schedule_id", type=str, required=True, help="Id of the schedule")
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
        parser.add_argument("--schedule_id", type=str, required=True, help="Id of the schedule")
        parser.add_argument("--schedule_description", type=str, help="Schedule description and comments")
        parser.add_argument("--server_id", type=int, help="Id of the server where the schedule will run")
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
        parser.add_argument("--parameters", type=str, help="Parameters for exec_command")
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
        parser.add_argument("--schedule_id", type=str, required=True, help="Id of the schedule")
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
            "--exclude_disabled_servers",
            default=False,
            action="store_true",
            help="Exclude disabled servers from target server_ids",
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
        parser.add_argument("--text", type=str, required=True, help="Text to send to Slack")
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        ping_slack.main(args.text)

    @staticmethod
    def list_schedule_ids():
        """List schedule ids of all schedules"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="List schedule ids of all schedules",
        )
        if len(sys.argv) >= 3:
            parser.print_help(sys.stdout)
            sys.exit(0)
        list_schedules.main()

    @staticmethod
    def delete_schedule():
        """Delete a schedule using schedule_id"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Delete a schedule using schedule_id",
        )
        parser.add_argument("--schedule_id", type=str, required=True, help="Id of the schedule")
        # now that we're inside a subcommand, ignore the first TWO args
        args = parser.parse_args(sys.argv[2:])
        delete_schedule.main(args.schedule_id)

    @staticmethod
    def smart_schedule():
        """Generate smart schedules for a server using genetic algorithm, or rollback/manage blocklist"""
        parser = argparse.ArgumentParser(
            allow_abbrev=False,
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Generate smart schedules for a server using genetic algorithm, or rollback previous changes, or manage schedule blocklist",
        )

        # Subcommands: optimise, rollback, blocklist
        subparsers = parser.add_subparsers(dest="action", help="Action to perform. Options: optimise (default), rollback, or blocklist")

        # (Default) optimise subcommand
        optimise_parser = subparsers.add_parser(
            "optimise",
            help="optimise schedules using genetic algorithm",
            add_help=True,
        )
        optimise_parser.add_argument("--server_id", type=int, required=False, help="ID of the server")

        # Optional GA Configurations
        ga_config = optimise_parser.add_argument_group("ga_config", "Optional configurations for the genetic algorithm optimiser")
        ga_config.add_argument("--num_generations",type=int,required=False, help="Number of generations for the genetic algorithm. Default: 20")
        ga_config.add_argument("--sol_per_pop",type=int,required=False, help="Number of solutions per population for the genetic algorithm. Default: 40")
        ga_config.add_argument("--num_parents_mating",type=int,required=False, help="Number of parents mating for the genetic algorithm. Default: 10")
        ga_config.add_argument("--mutation_percent_genes",type=int,required=False, help="Mutation percentage of genes for the genetic algorithm. Default: 20")
        ga_config.add_argument("--parent_selection_type",type=str,required=False, help="Parent selection type for the genetic algorithm. Allowed values: ['sss', 'rws', 'sus', 'tournament', 'rank', 'random']. Default: rank")
        ga_config.add_argument("--crossover_type",type=str,required=False, help="Crossover type for the genetic algorithm. Allowed values: ['single_point', 'two_point', 'uniform']. Default: uniform")
        ga_config.add_argument("--mutation_type",type=str,required=False, help="Mutation type for the genetic algorithm. Allowed values: ['random', 'swap', 'inversion', 'scramble']. Default: random")
        ga_config.add_argument("--keep_elitism",type=int,required=False, help="Number of elite solutions to keep for the next generation. Default: 2")
        ga_config.add_argument("--random-seed",type=int,required=False, help="Set a random seed to get repeatable results. Default: None")

        # Rollback subcommand
        rollback_parser = subparsers.add_parser(
            "rollback",
            help="Rollback to original or previous cron schedules",
            add_help=True,
            prog=inspect.stack()[0][3],
            description="Rollback for smart scheduling, it resets the schedule to its original cron in case of any issues",
        )

        # Mutually exclusive flags for rollback mode
        rollback_mode = rollback_parser.add_mutually_exclusive_group(required=True)
        rollback_mode.add_argument(
            "--full",
            default=False,
            action="store_true",
            help="Rollback to original schedule (set smart_interval_mask to NULL)",
        )
        rollback_mode.add_argument(
            "--previous",
            default=False,
            action="store_true",
            help="Rollback to most recent snapshot (step back one optimization)",
        )

        # Add mutually exclusive arguments for rollback subcommand to specify either server_id or schedule_id for targeted rollback
        group = rollback_parser.add_mutually_exclusive_group()
        group.add_argument(
            "--server_id",
            type=int,
            required=False,
            help="ID of the server to rollback, if not specified will rollback all servers",
        )
        group.add_argument("--schedule_id", type=str, required=False, help="ID of the schedule to rollback")


        # Blocklist subcommand
        blocklist_parser = subparsers.add_parser(
            "blocklist",
            help="Add or remove a schedule from the blocklist (excluded from smart scheduling optimization)",
            add_help=True,
        )
        blocklist_parser.add_argument(
            "--schedule_id",
            type=str,
            required=True,
            help="Id of the schedule to blocklist/unblocklist",
        )
        blocklist_parser.add_argument(
            "--remove",
            default=False,
            action="store_true",
            help="Remove the schedule from the blocklist instead of adding it",
        )
        blocklist_parser.add_argument(
            "--reason",
            type=str,
            required=False,
            help="Reason for blocklisting (optional, only used when adding)",
        )

        # Parse arguments and call smart_schedule.main with appropriate arguments based on subcommand
        args = parser.parse_args(sys.argv[2:])

        if args.action == "optimise" or args.action is None:
            smart_schedule.main(
                server_id=getattr(args, 'server_id', None),
                ga_config={
                    "num_generations": getattr(args, 'num_generations', None),
                    "sol_per_pop": getattr(args, 'sol_per_pop', None),
                    "num_parents_mating": getattr(args, 'num_parents_mating', None),
                    "mutation_percent_genes": getattr(args, 'mutation_percent_genes', None),
                    "parent_selection_type": getattr(args, 'parent_selection_type', None),
                    "crossover_type": getattr(args, 'crossover_type', None),
                    "mutation_type": getattr(args, 'mutation_type', None),
                    "keep_elitism": getattr(args, 'keep_elitism', None),
                    "random_seed": getattr(args, 'random_seed', None),
                },
            )
        elif args.action == "rollback":
            smart_schedule.main(
                server_id=getattr(args, 'server_id', None),
                schedule_id=getattr(args, 'schedule_id', None),
                rollback=True,
                full=getattr(args, 'full', False),
                previous=getattr(args, 'previous', False),
            )
        elif args.action == "blocklist":
            blocklist_schedule_cmd.main(
                schedule_id=args.schedule_id,
                remove=getattr(args, 'remove', False),
                reason=getattr(args, 'reason', None),
            )

    @staticmethod
    def version():
        """Return version of cicada package"""
        print(get_distribution("cicada").version)


def main():
    """Cicada agent CLI."""
    Cicada()
