import argparse
import subprocess

from ros2bag.verb import VerbExtension


class ExportVerb(VerbExtension):
    """Export a bag from the local cache to a directory."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina export` (e.g. <bag_ref> <output_dir>)',
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'export'] + args.args)
        return result.returncode
