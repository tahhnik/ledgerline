"""Checks import the learner's files from the lab folder: LAB_DIR, or the
current directory (run pytest from ~/lab)."""
import os
import sys

sys.path.insert(0, os.environ.get("LAB_DIR", os.getcwd()))
