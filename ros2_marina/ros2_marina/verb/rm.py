import argparse
import subprocess

from ros2bag.verb import VerbExtension


class RmVerb(VerbExtension):
    """Remove a bag from the local cache."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina rm` (e.g. <bag_ref>)',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'rm'] + args.args)
        return result.returncode
