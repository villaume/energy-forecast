from __future__ import annotations

import subprocess
import sys


def _run(cmd: list[str]) -> int:
    result = subprocess.run(cmd)
    return result.returncode


def main() -> None:
    steps = [
        ["ruff", "check", "."],
        ["black", "."],
        ["ty", "check", "src", "tests"],
    ]
    for step in steps:
        code = _run(step)
        if code != 0:
            sys.exit(code)


if __name__ == "__main__":
    main()
