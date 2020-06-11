from subprocess import call

import click


@click.command(help="Run isort and black over the whole project")
def cli():
    run_command("isort", "isort", "-y")
    run_command("black", "black", ".")


def run_command(name, *args, **kwargs) -> int:
    exit_code = call(args, **kwargs)

    if exit_code > 0:
        raise click.ClickException(
            f"'{name}' command exited with exit code {exit_code}"
        )
