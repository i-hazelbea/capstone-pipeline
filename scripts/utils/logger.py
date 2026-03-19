"""Logging utilities for ingestion scripts."""

from __future__ import annotations

import logging
import os
import sys


def setup_logger(name: str, log_level: str | None = None) -> logging.Logger:
    """Create or return a configured logger instance."""
    # Log Level Name in String
    level_name = (log_level or os.getenv("LOG_LEVEL", "INFO")).upper()

    # Log Level as Numerical Constant used by Python
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        for handler in logger.handlers:
            handler.setLevel(level)

    return logger
