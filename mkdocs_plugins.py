import ast
from io import StringIO
from pathlib import Path

from mkdocs.plugins import BasePlugin


class LiftBlock(BasePlugin):
    config_scheme = ()

    def on_page_markdown(self, markdown, page, config, files):
        if "<lift-blocks>" in markdown:
            io = StringIO()
            project_path = Path(config["config_file_path"]).parent
            blocks_path = project_path / "getl" / "blocks"
            write_file(blocks_path, io)
            return markdown.replace("<lift-blocks>", io.getvalue())
        return markdown


def write_file(src: Path, dest_file):
    for entrypoint_path in sorted(src.glob("*/entrypoint.py")):
        module_name = entrypoint_path.parent.name
        module = ast.parse(entrypoint_path.read_text())
        module_doc = ast.get_docstring(module)
        dest_file.write(f"## {module_name}\n\n{module_doc}\n\n")

        for node in module.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            func_name = node.name
            if not valid_func_name(func_name):
                continue
            func_doc = ast.get_docstring(node)
            dest_file.write(f"### {module_name}::{func_name}\n\n{func_doc}\n\n")


def valid_func_name(func_name: str) -> bool:
    if func_name.startswith("_"):
        return False
    if func_name == "resolve":
        return False
    return True
