import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class ImportVerb(VerbExtension):
    """Add a local bag to the cache, or prepare a directory for ros2 bag record."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina import` (e.g. <name> [path])',
        )

    def main(self, *, args):
        return run_marina('import', args.args)
