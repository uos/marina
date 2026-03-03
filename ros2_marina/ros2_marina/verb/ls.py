import argparse
import subprocess

from ros2bag.verb import VerbExtension


class LsVerb(VerbExtension):
    """List bags in marina registries."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina ls` (e.g. [--remote] [-r <registry>] [<pattern>])',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'ls'] + args.args)
        return result.returncode
