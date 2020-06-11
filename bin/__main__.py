from pathlib import Path

import click

BIN_DIR = Path(__file__).parent
PLUGIN_DIR = BIN_DIR / "commands"


class MyCLI(click.MultiCommand):
    def list_commands(self, ctx):
        return sorted(
            command_file.stem.replace("_", "-")
            for command_file in PLUGIN_DIR.glob("*.py")
        )

    def get_command(self, ctx, name):
        file_name = name.replace("-", "_")
        command_file = PLUGIN_DIR / f"{file_name}.py"
        if not command_file.exists():
            raise click.ClickException(f"Command {name} not found")
        ns = {"__file__": str(command_file.absolute())}
        code = compile(command_file.read_text(), str(command_file.absolute()), "exec")
        eval(code, ns, ns)
        return ns["cli"]


cli = MyCLI(help="This tool's subcommands are loaded from a plugin folder dynamically.")

if __name__ == "__main__":
    cli()
