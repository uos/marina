import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class InspectVerb(VerbExtension):
    """Show metadata and file listing for a dataset."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina inspect` (e.g. <dataset> [--registry <name>])',
        )

    def main(self, *, args):
        return run_marina('inspect', args.args)
