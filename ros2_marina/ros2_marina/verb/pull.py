import argparse
import subprocess
import sys

from ros2bag.verb import VerbExtension


class PullVerb(VerbExtension):
    """Pull a bag from a marina registry."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina pull` (e.g. <bag_ref> [-o <dir>] [-r <registry>])',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'pull'] + args.args)
        return result.returncode
