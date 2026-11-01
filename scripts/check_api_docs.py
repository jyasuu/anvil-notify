#!/usr/bin/env python3
"""Fail when docs/openapi.yaml and the axum routers disagree.

Checks, in order:
  1. docs/openapi.yaml is a valid OpenAPI document.
  2. Every `.route("<path>", get(..).post(..))` in the api crate has a matching
     path + method in the spec, and the spec has no operation without a route.

Usage:  pip install pyyaml openapi-spec-validator && python3 scripts/check_api_docs.py
"""
import pathlib
import re
import sys

import yaml
from openapi_spec_validator import validate

ROOT = pathlib.Path(__file__).resolve().parent.parent
ROUTE_FILES = [ROOT / "crates/api/src/routes.rs", ROOT / "crates/api/src/docs.rs"]
METHODS = {"get", "post", "put", "patch", "delete"}

# `.route("/x/{id}", get(h).post(h2))` — the handler part may contain one level of parens.
ROUTE_RE = re.compile(r'\.route\(\s*"([^"]+)"\s*,\s*((?:[^()]|\([^()]*\))*)\)')
METHOD_RE = re.compile(r"\b(get|post|put|patch|delete)\(")


def code_routes() -> set[tuple[str, str]]:
    found = set()
    for f in ROUTE_FILES:
        text = re.sub(r"//[^\n]*", "", f.read_text())  # ignore commented-out routes
        for path, handlers in ROUTE_RE.findall(text):
            for m in METHOD_RE.findall(handlers):
                found.add((m, path))
    return found


def spec_routes(spec: dict) -> set[tuple[str, str]]:
    return {
        (m, path)
        for path, item in spec["paths"].items()
        for m in item
        if m in METHODS
    }


def main() -> int:
    spec = yaml.safe_load((ROOT / "docs/openapi.yaml").read_text())
    validate(spec)

    code, documented = code_routes(), spec_routes(spec)
    if not code:
        print("no routes found in the api crate — has the router moved?", file=sys.stderr)
        return 2

    undocumented = sorted(code - documented)
    stale = sorted(documented - code)
    for m, p in undocumented:
        print(f"route without spec entry : {m.upper():6} {p}")
    for m, p in stale:
        print(f"spec entry without route : {m.upper():6} {p}")
    if undocumented or stale:
        return 1
    print(f"openapi.yaml OK — {len(code)} operations match the router")
    return 0


if __name__ == "__main__":
    sys.exit(main())
