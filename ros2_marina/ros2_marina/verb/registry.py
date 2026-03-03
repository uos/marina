import argparse
import subprocess

from ros2bag.verb import VerbExtension


class RegistryVerb(VerbExtension):
    """Manage marina registries.

    Subcommands:
      add <uri> [--name <name>] [--kind <kind>] [--auth-env <var>]
      list (alias: ls)
      rm <name> [--delete-data]
      auth <name> [--client-id <id>]
      mirror <source> <target>
    """

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            'args',
            nargs=argparse.REMAINDER,
            help=(
                'Subcommand and arguments forwarded to `marina registry` '
                '(add | list | rm | auth | mirror)'
            ),
        )

    def main(self, *, args):
        result = subprocess.run(['marina', 'registry'] + args.args)
        return result.returncode
