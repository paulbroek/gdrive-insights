import argparse

from rarc_utils.log import loggingLevelNames


class ArgParser:
    """Create CLI parser."""

    @staticmethod
    def create_parser():
        return argparse.ArgumentParser()

    @classmethod
    def get_parser(cls):

        CLI = cls.create_parser()

        CLI.add_argument(
            "-v",
            "--verbosity",
            type=str,
            default="info",
            help=f"choose debug log level: {', '.join(loggingLevelNames())}",
        )
        CLI.add_argument(
            "--use_cache",
            action="store_true",
            help="load changes from feather cache, and append to changes to fetch list",
        )
        CLI.add_argument(
            "--use_cache_sql",
            action="store_true",
            help="load changes from sql cache, and append to changes to fetch list",
        )
        CLI.add_argument(
            "-s",
            "--save",
            action="store_true",
            help="save changes to feather file",
        )
        CLI.add_argument(
            "-p",
            "--push",
            action="store_true",
            help="push files and revisions to db",
        )
        CLI.add_argument(
            "-n",
            "--nfetch",
            type=int,
            default=2,
            help="max number of API calls to do. one call fetches 100 change items",
        )
        CLI.add_argument(
            "-t",
            "--start_page_token",
            type=int,
            default=209,
            help="start_page_token to start polling from (low number will always start from first change in time)",
        )
        CLI.add_argument(
            "--dryrun",
            action="store_true",
            default=False,
            help="Only load browser and login, do nothing else",
        )

        return CLI
