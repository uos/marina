import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class CleanVerb(VerbExtension):
    """Clean the local marina cache."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina clean` (e.g. [--all])',
        )

    def main(self, *, args):
        return run_marina('clean', args.args)
