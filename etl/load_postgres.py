"""Backward-compatible PostgreSQL entry point.

Use ``python3 load.py --target postgres`` for new work. Keeping this tiny
wrapper avoids two diverging PostgreSQL loader implementations.
"""

import logging
import os

from extract import extract_all
from load import load_all
from transform import transform_all
from validate import validate_all


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    tables = transform_all(extract_all())
    validate_all(tables)
    load_all(tables, target="postgres", database_url=os.environ.get("DATABASE_URL"))
