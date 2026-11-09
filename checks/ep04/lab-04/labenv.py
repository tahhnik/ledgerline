"""Connection settings for the Lab 04 checks."""
from __future__ import annotations

import os
from pathlib import Path


def _find_bootstrap() -> str:
    if "BOOTSTRAP" in os.environ:
        return os.environ["BOOTSTRAP"]
    lab_dir = Path(os.environ.get("LAB_DIR", os.getcwd()))
    env_file = lab_dir / "aws.env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("BOOTSTRAP="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return "localhost:9092"


BOOTSTRAP = _find_bootstrap()
