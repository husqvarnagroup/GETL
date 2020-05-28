import ast
from io import StringIO
from operator import attrgetter
from pathlib import Path
from typing import Iterable

from mkdocs.config.config_options import Type
from mkdocs.plugins import BasePlugin
from mkdocs.structure.files import File


class RootFiles(BasePlugin):
    config_scheme = (("files", Type(list, default=[])),)

    def on_files(self, files, config):
        project_path = Path(config["config_file_path"]).parent
        for filename in self.config["files"]:
            files.append(
                File(
                    filename,
                    project_path,
                    config["site_dir"],
                    config["use_directory_urls"],
                )
            )

        return files

    def on_serve(self, server, config, builder):
        project_path = Path(config["config_file_path"]).parent
        for filename in self.config["files"]:
            server.watch(str(project_path / filename), builder)
        return server

    def on_pre_page(self, page, config, files):
        # Change the edit urls for ROOT_FILES
        project_path = Path(config["config_file_path"]).parent
        for filename in self.config["files"]:
            root_file = project_path / filename
            if root_file.samefile(page.file.abs_src_path):
                page.edit_url = f"{config['repo_url']}edit/master/{filename}"
        return page


class LiftBlock(BasePlugin):
    config_scheme = ()

    def get_blocks_path(self, config):
        project_path = Path(config["config_file_path"]).parent
        return project_path / "getl" / "blocks"

    def on_serve(self, server, config, builder):
        server.watch(str(self.get_blocks_path(config)), builder)
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
