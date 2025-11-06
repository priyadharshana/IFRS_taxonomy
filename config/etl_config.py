"""
Configuration module for ETL process.
"""

import logging
import sys
from pathlib import Path


def setup_logger(
    name: str, log_path: Path, level: int = logging.INFO
) -> logging.Logger:
    """
    Sets up and returns a logger, avoiding duplicate handlers.

    Args:
        name: The name for the logger.
        log_path: The full path to the log file.
        level: The logging level.

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers if they already exist
    if not logger.handlers:
        # Ensure the logs directory exists
        log_path.parent.mkdir(parents=True, exist_ok=True)

        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(log_format)

        # Add file handler
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Add stream handler to output to console
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        logger.info(
            f"Logger '{name}' initialized with level {logging.getLevelName(level)}"
        )

    return logger
