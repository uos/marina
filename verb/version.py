from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class VersionVerb(VerbExtension):
    """Print the marina version."""

    def add_arguments(self, parser, cli_name):
        pass

    def main(self, *, args):
        return run_marina('version')
