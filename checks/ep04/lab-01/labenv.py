"""Connection settings for the Lab 01 checks."""
import os

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
