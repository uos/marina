import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class PushVerb(VerbExtension):
    """Push a bag to a marina registry."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina push` (e.g. <name> <path> [-r <registry>])',
        )

    def main(self, *, args):
        return run_marina('push', args.args)
