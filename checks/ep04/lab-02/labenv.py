"""Connection settings for the Lab 02 checks: BOOTSTRAP lists the three brokers'
public addresses (set by the Environment Setup in ~/.bashrc)."""
import os

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9094")
