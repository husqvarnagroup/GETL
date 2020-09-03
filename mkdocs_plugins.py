import ast
import re
from io import StringIO
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import Iterable, Iterator, List, Tuple

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

    def on_serve(self, server, config, builder=None):
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
        return project_path / "getl"

    def on_serve(self, server, config, builder=None):
        server.watch(str(self.get_blocks_path(config)), builder)
        return server

    def on_page_markdown(self, markdown, page, config, files):
        if "<lift-blocks>" in markdown:
            doc = generate_docs(
                entrypoint_files(self.get_blocks_path(config) / "blocks"),
                delimiter="::",
            )
            return markdown.replace("<lift-blocks>", doc)

        if "<transform-functions>" in markdown:
            doc = generate_docs(
                transform_files(self.get_blocks_path(config) / "blocks" / "transform"),
                delimiter=".",
            )
            return markdown.replace("<transform-functions>", doc)

        if "<file-registry>" in markdown:
            doc = generate_docs(
                entrypoint_files(self.get_blocks_path(config)),
                delimiter="::",
                add_header=False,
            )
            return markdown.replace("<file-registry>", doc)

        return markdown


def generate_docs(
    modules: Iterable[Tuple[str, ast.Module]], delimiter: str, add_header: bool = True
) -> str:
    dest_file = StringIO()

    for module_name, module in sorted(modules, key=itemgetter(0)):
        function_nodes = function_nodes_in_module(module)

        if not function_nodes:
            # Only document the module if there are functions defined in the module
            continue

        module_doc = ast.get_docstring(module)

        if add_header:
            dest_file.write(f"## {module_name}\n\n{module_doc}\n\n")

        for node in function_nodes:
            func_name = node.name
            func_path = f"{module_name}{delimiter}{func_name}"
            func_doc = format_function_docstring(ast.get_docstring(node))
            dest_file.write(f"### {func_path}\n\n{func_doc}\n\n")

    return dest_file.getvalue()


def entrypoint_files(base_path: Path) -> Iterator[Tuple[str, ast.Module]]:
    python_files = base_path.glob("*/entrypoint.py")

    for python_file in python_files:
        module_name = python_file.parent.name
        module = ast.parse(python_file.read_text())

        yield module_name, module


def transform_files(base_path: Path) -> Iterator[Tuple[str, ast.Module]]:
    python_files = base_path.rglob("*.py")

    for python_file in python_files:
        rel = list(python_file.parts[len(base_path.parts) :])

        if rel[-1] == "__init__.py":
            # Remove __init__.py from module path
            rel.pop(-1)
        elif rel[0] == "entrypoint.py":
            # entrypoint.py should not be documented here
            continue
        if rel and rel[-1].endswith(".py"):
            rel[-1] = rel[-1][:-3]

        module_name = ".".join(rel)
        module = ast.parse(python_file.read_text())

        yield module_name, module


def function_nodes_in_module(module: ast.Module) -> List[ast.FunctionDef]:
    return sorted(
        (
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and valid_func_name(node.name)
        ),
        key=attrgetter("name"),
    )


def format_function_docstring(docstring: str) -> str:
    word_match = "([a-zA-Z0-9_. -]+)"
    start_string = rf":param ([\w\[\]]+) {word_match}"

    regex_list = [
        # Add parameter heading over each paramter group
        (r"\n\n:param", "\n\nParameters\n:   \n:param"),
        # Format parameters with no optional or default tag
        (rf"{start_string}:\s*(.*)$", r"- **\2** (*\1*) – \3"),
        # Format paramters with optional and default tag
        (
            rf"{start_string}={word_match}:\s*(.*)$",
            r"- **\2** (*\1, optional, default: \3*) – \4",
        ),
        # Format the rest of the paramters that only have a optional tag
        (rf"{start_string}=:\s*(.*)$", r"- **\2** (*\1, optional*) – \3"),
    ]

    for search, replace in regex_list:
        docstring = re.sub(search, replace, docstring, flags=re.MULTILINE)

    return docstring


def valid_func_name(func_name: str) -> bool:
    if func_name.startswith("_"):
        return False
    if func_name == "resolve":
        return False
    return True
