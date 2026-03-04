import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class PullVerb(VerbExtension):
    """Pull a bag from a marina registry."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina pull` (e.g. <bag_ref> [-o <dir>] [-r <registry>])',
        )

    def main(self, *, args):
        return run_marina('pull', args.args)
