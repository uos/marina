import argparse
import subprocess

from ros2bag.verb import VerbExtension


class SearchVerb(VerbExtension):
    """Search for bags across marina registries."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina search` (e.g. <pattern> [-r <registry>])',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'search'] + args.args)
        return result.returncode
