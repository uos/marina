import os
import shutil
import subprocess
from importlib import resources


def _packaged_marina_executable():
    bin_name = 'marina.exe' if os.name == 'nt' else 'marina'
    try:
        candidate = resources.files('ros2_marina').joinpath('_bin', bin_name)
        if candidate.is_file():
            return str(candidate)
    except Exception:
        return None
    return None


def run_marina(subcommand, forwarded_args=None):
    forwarded_args = forwarded_args or []
    executable = _packaged_marina_executable() or shutil.which('marina') or 'marina'
    env = os.environ.copy()
    env['MARINA_PROG_NAME'] = 'ros2 bag'
    result = subprocess.run([executable, subcommand] + forwarded_args, env=env)
    return result.returncode
