import re
from enum import Enum
from pathlib import Path
from subprocess import call

import click

RE_VERSION = re.compile(r'version = "(?P<version>\d+\.\d+(\.\d+)?)"')
PROJECT_DIR = Path(__file__).parent.parent.parent


def get_version():
    pyproject_toml = PROJECT_DIR / "pyproject.toml"
    match = RE_VERSION.search(pyproject_toml.read_text())
    if not match:
        raise ValueError(f"Could not find version in {pyproject_toml}")
    return match.group("version")


def set_version(version_number: str):
    pyproject_toml = PROJECT_DIR / "pyproject.toml"
    file_text = pyproject_toml.read_text()
    new_file_text = RE_VERSION.sub(f'version = "{version_number}"', file_text)
    pyproject_toml.write_text(new_file_text)


@click.command(help="Bumps version and creates a git commit + tag")
@click.argument("version_part", default="patch")
@click.option("--dry-run", is_flag=True)
def cli(version_part, dry_run):
    version_part = VersionPart(version_part)
    old_version = get_version()
    version = increase_version_number(old_version, version_part)

    click.echo(f"Bumping version from v{old_version} to v{version}")
    if not dry_run:
        set_version(version)

    if not dry_run:
        run_command(
            "git commit",
            "git",
            "commit",
            "-p",
            "-m",
            f"Bump version from v{old_version} to v{version}",
        )

        run_command("git tag", "git", "tag", f"v{version}")


class VersionPart(Enum):
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"

    def __str__(self):
        return self.value


def increase_version_number(version_number: str, version_part: VersionPart) -> str:
    version_split = list(map(int, version_number.split(".")))

    if len(version_split) == 2:
        version_split.append(0)

    if version_part is VersionPart.PATCH:
        version_split[2] += 1
    elif version_part is VersionPart.MINOR:
        version_split[1] += 1
        version_split[2] = 0
    elif version_part is VersionPart.MAJOR:
        version_split[0] += 1
        version_split[1] = 0
        version_split[2] = 0

    return ".".join(map(str, version_split))


def run_command(name, *args, **kwargs) -> int:
    exit_code = call(args, **kwargs)

    if exit_code > 0:
        raise click.ClickException(
            f"'{name}' command exited with exit code {exit_code}"
        )
