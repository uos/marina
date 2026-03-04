import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class LsVerb(VerbExtension):
    """List bags in marina registries."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina ls` (e.g. [--remote] [-r <registry>] [<pattern>])',
        )

    def main(self, *, args):
        return run_marina('ls', args.args)
