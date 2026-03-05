from ros2bag.verb import VerbExtension
from ros2_marina._marina_exec import run_marina


class LsVerb(VerbExtension):
    """List bags in marina registries."""

    def add_arguments(self, parser, cli_name):
        parser.add_argument(
            '--remote',
            action='store_true',
            default=False,
            help='List datasets available in all remote registries instead of the local cache',
        )
        parser.add_argument(
            '--registry', '-r',
            help='Filter to a specific registry (only with --remote)',
        )

    def main(self, *, args):
        forwarded = []
        if args.remote:
            forwarded.append('--remote')
        if args.registry:
            forwarded.extend(['--registry', args.registry])
        return run_marina('ls', forwarded)
