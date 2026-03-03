import argparse
import subprocess

from ros2bag.verb import VerbExtension


class PushVerb(VerbExtension):
    """Push a bag to a marina registry."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina push` (e.g. <name> <path> [-r <registry>])',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'push'] + args.args)
        return result.returncode
