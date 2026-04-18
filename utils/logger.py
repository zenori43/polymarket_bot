"""
utils/logger.py

Centralised loguru configuration.
- Format : {time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} | {message}
- Output : console (stderr) + logs/bot.log
- Rotation: every 10 MB
- Retention: 7 days
"""

import sys
import os
from loguru import logger as _logger
from loguru._logger import Logger

_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure logs directory exists relative to the project root.
# __file__ is  <project_root>/utils/logger.py  →  parent is <project_root>
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_LOG_FILE = os.path.join(_PROJECT_ROOT, "logs", "bot.log")
os.makedirs(os.path.dirname(_LOG_FILE), exist_ok=True)

_LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} | {message}"

# Remove the default loguru handler so we control everything ourselves
_logger.remove()

# Console sink
_logger.add(
    sys.stderr,
    format=_LOG_FORMAT,
    level=_LOG_LEVEL,
    colorize=True,
    backtrace=True,
    diagnose=True,
)

# File sink
_logger.add(
    _LOG_FILE,
    format=_LOG_FORMAT,
    level="DEBUG",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    backtrace=True,
    diagnose=True,
    encoding="utf-8",
)


def get_logger(name: str) -> Logger:
    """
    Return a loguru Logger bound with the given *name* so that every log line
    shows the caller module name in the {name} field.

    Usage::

        from utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("hello")
    """
    return _logger.bind(name=name)
