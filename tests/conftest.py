"""Ambiente pytest: interpretador PySpark (útil em venv, Cloud9, AWS Academy)."""

from __future__ import annotations

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

if sys.platform == "win32":
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "127.0.0.1")
