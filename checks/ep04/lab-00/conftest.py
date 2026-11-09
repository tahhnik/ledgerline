"""Checks import the learner's files from the lab folder (LAB_DIR, or the
current directory: run pytest from ~/lab) and the provided Lab 00 helpers."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "provided/ep04/lab-00"))
sys.path.insert(0, os.environ.get("LAB_DIR", os.getcwd()))
