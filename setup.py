import os
import shutil
import stat
import subprocess
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install


THIS_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = THIS_DIR
PACKAGE_BIN_DIR = THIS_DIR / '_bin'
BIN_NAME = 'marina.exe' if os.name == 'nt' else 'marina'


def _build_marina_binary():
	cmd = ['cargo', 'build', '--release', '-p', 'marina']
	try:
		subprocess.run(cmd, cwd=WORKSPACE_ROOT, check=True)
	except FileNotFoundError as exc:
		raise RuntimeError('cargo not found. Install Rust toolchain to build marina.') from exc

	built_binary = WORKSPACE_ROOT / 'target' / 'release' / BIN_NAME
	if not built_binary.exists():
		raise RuntimeError(f'expected built marina binary at {built_binary}, but it was not found')

	PACKAGE_BIN_DIR.mkdir(parents=True, exist_ok=True)
	packaged_binary = PACKAGE_BIN_DIR / BIN_NAME
	shutil.copy2(built_binary, packaged_binary)
	packaged_binary.chmod(packaged_binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class build_py(_build_py):
	def run(self):
		self.announce('building marina Rust binary', level=2)
		_build_marina_binary()
		super().run()


class install(_install):
	def run(self):
		self.announce('building marina Rust binary', level=2)
		_build_marina_binary()
		super().run()


class develop(_develop):
	def run(self):
		self.announce('building marina Rust binary', level=2)
		_build_marina_binary()
		super().run()


setup(
	cmdclass={
		'build_py': build_py,
		'install': install,
		'develop': develop,
	}
)
