import ast
from io import StringIO
from operator import attrgetter
from pathlib import Path
from typing import Iterable

from mkdocs.plugins import BasePlugin


class LiftBlock(BasePlugin):
    config_scheme = ()

    def get_blocks_path(self, config):
        project_path = Path(config["config_file_path"]).parent
        return project_path / "getl" / "blocks"

    def on_serve(self, server, config, builder):
        server.watch(self.get_blocks_path(config), builder)
        return server

    def on_page_markdown(self, markdown, page, config, files):
        if "<lift-blocks>" in markdown:
            doc = generate_entrypoint_docs(
                self.get_blocks_path(config).glob("*/entrypoint.py")
            )
            return markdown.replace("<lift-blocks>", doc)
        return markdown


def generate_entrypoint_docs(python_files: Iterable[Path]) -> str:
    dest_file = StringIO()
    for entrypoint_path in sorted(python_files, key=attrgetter("parent.name")):
        module_name = entrypoint_path.parent.name
        module = ast.parse(entrypoint_path.read_text())
        module_doc = ast.get_docstring(module)
        dest_file.write(f"## {module_name}\n\n{module_doc}\n\n")

        function_nodes = sorted(
            (
                node
                for node in module.body
                if isinstance(node, ast.FunctionDef) and valid_func_name(node.name)
            ),
            key=attrgetter("name"),
        )

        for node in function_nodes:
            func_name = node.name
            func_doc = ast.get_docstring(node)
            dest_file.write(f"### {module_name}::{func_name}\n\n{func_doc}\n\n")
    return dest_file.getvalue()


def valid_func_name(func_name: str) -> bool:
    if func_name.startswith("_"):
        return False
    if func_name == "resolve":
        return False
    return True
