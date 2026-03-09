import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class ResolveVerb(VerbExtension):
    """Resolve a bag reference to a local path, downloading if necessary."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina resolve` (e.g. <bag_ref>)',
        )

    def main(self, *, args):
        return run_marina('resolve', args.args)
