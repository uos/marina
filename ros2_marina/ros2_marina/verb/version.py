import subprocess

from ros2bag.verb import VerbExtension


class VersionVerb(VerbExtension):
    """Print the marina version."""

    def add_arguments(self, parser, cli_name):
        pass

    def main(self, *, args):
        result = subprocess.run(['marina', 'version'])
        return result.returncode
