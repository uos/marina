import argparse
import subprocess

from ros2bag.verb import VerbExtension


class ResolveVerb(VerbExtension):
    """Resolve a bag reference to a local path, downloading if necessary."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina resolve` (e.g. <bag_ref>)',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'resolve'] + args.args)
        return result.returncode
