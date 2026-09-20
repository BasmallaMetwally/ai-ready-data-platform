"""
logging_config.py
------------------
إعداد Logging مركزي للـ pipeline كله: بيكتب في الـ console وفي ملف
logs/etl.log مع Rotation (عشان الملف ميكبرش من غير نهاية).
"""

import logging
import os
from logging.handlers import RotatingFileHandler

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, "etl.log")


def setup_logging(level=logging.INFO):
    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()  # نمنع تكرار الـ handlers لو الدالة اتنادت أكتر من مرة

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    return root
