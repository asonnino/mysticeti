#!/usr/bin/env python3
# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail if any Lean source contains a proof hole or soundness escape hatch.

The kernel guarantees that every proof that *exists* is correct; the
constructs below let a file build while smuggling in unproven assumptions:

    sorry / admit     accept a goal without proof (warning only, build stays green)
    axiom             assume a statement with no proof at all
    native_decide     trust compiled code instead of the kernel
    unsafe / partial  opt out of soundness / termination checking

Comments are stripped before matching, so prose may mention the words
(e.g. "partial synchrony") freely.

Additionally, every file named `Statement.lean` must be proof-free
(definitions and prose only): a theorem/lemma/example/instance there would
sit outside the audit convention that Statement files are read in full and
proof files not at all.

This script is part of the trusted base: it is meant to be read once,
top to bottom, and then believed. Keep it dumb.
"""

import re
import sys
from pathlib import Path

FORBIDDEN = re.compile(r"\b(sorry|admit|axiom|native_decide|unsafe|partial)\b")
STATEMENT_FORBIDDEN = re.compile(r"^\s*(theorem|lemma|example|instance)\b")
ROOT = Path(__file__).resolve().parent.parent
SOURCES = ["Hydrozoan.lean", "HydrozoanTest.lean", "Hydrozoan", "HydrozoanTest"]


def lean_files():
    for entry in SOURCES:
        path = ROOT / entry
        if path.is_file():
            yield path
        elif path.is_dir():
            yield from sorted(path.rglob("*.lean"))


def strip_comments(lines):
    """Yield (lineno, code) with line comments and (nesting) block comments removed."""
    depth = 0
    for lineno, line in enumerate(lines, start=1):
        code = []
        i = 0
        while i < len(line):
            if depth == 0 and line.startswith("--", i):
                break  # line comment: drop the rest of the line
            if line.startswith("/-", i):
                depth += 1
                i += 2
            elif depth > 0 and line.startswith("-/", i):
                depth -= 1
                i += 2
            elif depth == 0:
                code.append(line[i])
                i += 1
            else:
                i += 1  # inside a block comment
        yield lineno, "".join(code)


def main():
    holes = []
    for path in lean_files():
        lines = path.read_text(encoding="utf-8").splitlines()
        for lineno, code in strip_comments(lines):
            match = FORBIDDEN.search(code)
            if match:
                holes.append(f"{path.relative_to(ROOT)}:{lineno}: {match.group(0)}")
            if path.name == "Statement.lean":
                match = STATEMENT_FORBIDDEN.search(code)
                if match:
                    holes.append(
                        f"{path.relative_to(ROOT)}:{lineno}: "
                        f"{match.group(1)} (proof material in a Statement file)"
                    )
    if holes:
        print("Proof holes found:")
        for hole in holes:
            print(f"  {hole}")
        return 1
    print("No proof holes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
