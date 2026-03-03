import argparse
import subprocess

from ros2bag.verb import VerbExtension


class CleanVerb(VerbExtension):
    """Clean the local marina cache."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina clean` (e.g. [--all])',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'clean'] + args.args)
        return result.returncode
