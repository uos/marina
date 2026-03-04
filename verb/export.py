import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class ExportVerb(VerbExtension):
    """Export a bag from the local cache to a directory."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina export` (e.g. <bag_ref> <output_dir>)',
        )

    def main(self, *, args):
        return run_marina('export', args.args)
