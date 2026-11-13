#!/usr/bin/env python3
"""Transparent intake wrapper: identical observable behaviour to the real CLI.

Used to prove the configurable command-prefix mechanism itself introduces no
divergence.  It simply re-executes ``python -m lot_lineage.intake`` with the
same arguments and propagates its exit code and streams byte-for-byte.
"""
from __future__ import annotations

import subprocess
import sys


def main(argv: list[str]) -> int:
    completed = subprocess.run(
        [sys.executable, "-m", "lot_lineage.intake", *argv], check=False
    )
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
