"""Static guards for the package's declared Python 3.9 support."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def _uses_pep604(annotation: ast.AST | None) -> bool:
    return annotation is not None and any(
        isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr)
        for node in ast.walk(annotation)
    )


class _RuntimeAnnotationVisitor(ast.NodeVisitor):
    """Find PEP 604 annotations that Python 3.9 would evaluate at runtime."""

    def __init__(self) -> None:
        self.scope = ["module"]
        self.failures: list[tuple[int, str]] = []

    def _record(self, annotation: ast.AST | None) -> None:
        if _uses_pep604(annotation):
            self.failures.append(
                (getattr(annotation, "lineno", 0), ast.unparse(annotation))
            )

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        arguments = [
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ]
        if node.args.vararg is not None:
            arguments.append(node.args.vararg)
        if node.args.kwarg is not None:
            arguments.append(node.args.kwarg)
        for argument in arguments:
            self._record(argument.annotation)
        self._record(node.returns)

        self.scope.append("function")
        for statement in node.body:
            self.visit(statement)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self.scope.append("class")
        for statement in node.body:
            self.visit(statement)
        self.scope.pop()

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802
        # Function-local variable annotations are not evaluated. Module and
        # class annotations are, including a class nested inside a function.
        if self.scope[-1] in {"module", "class"}:
            self._record(node.annotation)
        if node.value is not None:
            self.visit(node.value)


def test_source_is_importable_under_python39_annotation_rules() -> None:
    source_root = Path(__file__).parents[1] / "src" / "maxc_cli"
    failures: list[str] = []

    for path in sorted(source_root.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path), feature_version=9)
        postponed = any(
            isinstance(node, ast.ImportFrom)
            and node.module == "__future__"
            and any(alias.name == "annotations" for alias in node.names)
            for node in tree.body
        )
        if postponed:
            continue
        visitor = _RuntimeAnnotationVisitor()
        visitor.visit(tree)
        failures.extend(
            f"{path.relative_to(source_root)}:{line}: {annotation}"
            for line, annotation in visitor.failures
        )

    assert not failures, (
        "Unquoted PEP 604 annotations break imports on Python 3.9; quote them "
        "or enable postponed annotations:\n" + "\n".join(failures)
    )
