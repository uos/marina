import argparse

from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class CompletionsVerb(VerbExtension):
    """Generate shell completion script for the marina commands."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help='Arguments forwarded to `marina completions` (e.g. bash | zsh | fish)',
        )

    def main(self, *, args):
        return run_marina('completions', args.args)
