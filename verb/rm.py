import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class RmVerb(VerbExtension):
    """Remove a bag from the local cache."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina rm` (e.g. <bag_ref>)',
        )

    def main(self, *, args):
        return run_marina('rm', args.args)
