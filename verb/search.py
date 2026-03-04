import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class SearchVerb(VerbExtension):
    """Search for bags across marina registries."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina search` (e.g. <pattern> [-r <registry>])',
        )

    def main(self, *, args):
        return run_marina('search', args.args)
