"""
Config Loader
--------------
Reads an external config.json instead of hardcoding everything (schema,
weights, primary key) inside main.py and scoring.py. This actually
separates config from code, so you can change validation settings without
touching a single line of code.
"""
import json
import logging

logger = logging.getLogger("dq_system.config")

DEFAULT_CONFIG_PATH = "config.json"


class ConfigError(Exception):
    """Raised when the config file is missing or contains invalid JSON."""
    pass


def load_config(path=DEFAULT_CONFIG_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        raise ConfigError(f"Config file '{path}' was not found. Check the path or use --config to point to a different one.")
    except json.JSONDecodeError as e:
        raise ConfigError(f"Config file '{path}' contains invalid JSON: {e}")

    # Convert date_pairs from list-of-lists (JSON) to list-of-tuples (what validation.py expects)
    if "date_pairs" in config:
        config["date_pairs"] = [tuple(pair) for pair in config["date_pairs"]]

    logger.info(f"Loaded config from {path}")
    return config
