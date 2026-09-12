"""Emit a reproducible lexical/AST inventory, not a SQL translator or linter."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
from pathlib import Path


PATHS = (
    "hermes_cli/kanban_db.py",
    "gateway/kanban_watchers.py",
    "hermes_cli/kanban.py",
)
PATTERN = (
    r"ORDER\s+BY|INSERT\s+OR\s+(?:REPLACE|IGNORE)|IS\s+\?|"
    r"datetime\s*\(|strftime|json_extract|GROUP_CONCAT|LIMIT\s+-1|"
    r"rowid|last_insert_rowid|PRAGMA|executescript|sqlite_master|"
    r"COLLATE|LOWER\(|UPPER\(|LIKE|GLOB|julianday|total_changes|\.backup\("
)


def inventory(path, source):
    tree = ast.parse(source, filename=path)
    parents = {
        child: node
        for node in ast.walk(tree)
        for child in ast.iter_child_nodes(node)
    }

    def owner(node):
        while node in parents:
            node = parents[node]
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return node.name
        return "<module>"

    nodes = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"execute", "executemany", "executescript"}
        ):
            nodes.append({
                "line": node.lineno, "kind": "sql", "owner": owner(node),
                "source": ast.unparse(node),
            })
        elif isinstance(node, ast.ExceptHandler):
            protected = parents[node]
            nodes.append({
                "line": node.lineno, "kind": "except", "owner": owner(node),
                "source": ast.unparse(node),
                "protected": ast.unparse(protected),
            })
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                expression = item.context_expr
                if isinstance(expression, ast.Call) and ast.unparse(expression.func).endswith("suppress"):
                    nodes.append({
                        "line": node.lineno, "kind": "suppress", "owner": owner(node),
                        "source": ast.unparse(node),
                    })
    return {
        "path": path,
        "sha256": hashlib.sha256(source.encode()).hexdigest(),
        "grep": [
            {"line": number, "source": line}
            for number, line in enumerate(source.splitlines(), start=1)
            if re.search(PATTERN, line, re.IGNORECASE)
        ],
        "nodes": sorted(nodes, key=lambda item: (item["line"], item["kind"])),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", help="Read this git revision; default: working tree")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    files = []
    for path in PATHS:
        source = (
            subprocess.run(
                ["git", "show", f"{args.revision}:{path}"],
                cwd=root, check=True, capture_output=True, text=True,
            ).stdout
            if args.revision else (root / path).read_text(encoding="utf-8")
        )
        files.append(inventory(path, source))
    print(json.dumps({"pattern": PATTERN, "files": files}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
