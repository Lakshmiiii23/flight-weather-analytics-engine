"""Structured JSON Logging Configuration for Cloud & Local Execution.

Emits standard-compliant JSON payloads for Google Cloud Logging,
container stdout collectors, and local dev readability.
"""

from datetime import datetime, timezone
import json
import logging
import sys
import traceback
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """Custom formatter outputting log records as single-line JSON objects."""

    def __init__(self, service_name: str = "flight-weather-engine") -> None:
        super().__init__()
        self.service_name: str = service_name

    def format(self, record: logging.LogRecord) -> str:
        """Format a LogRecord into a structured JSON string.

        Args:
            record: Standard Python logging LogRecord instance.

        Returns:
            JSON-formatted string representation of the log entry.
        """
        log_payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "service": self.service_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process_id": record.process,
            "thread_name": record.threadName,
        }

        # Include stack trace if an exception is present
        if record.exc_info:
            log_payload["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else "Unknown",
                "message": str(record.exc_info[1]) if record.exc_info[1] else "",
                "traceback": traceback.format_exception(*record.exc_info),
            }

        # Collect additional contextual attributes added via extra={}
        standard_attrs = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message"
        }
        extra_fields: Dict[str, Any] = {
            key: value
            for key, value in record.__dict__.items()
            if key not in standard_attrs and not key.startswith("_")
        }
        if extra_fields:
            log_payload["context"] = extra_fields

        return json.dumps(log_payload, default=str)


def setup_logging(
    log_level: str = "INFO",
    json_format: bool = True,
    service_name: str = "flight-weather-engine",
    logger_name: Optional[str] = None,
) -> logging.Logger:
    """Configures and returns a centralized, thread-safe logger.

    Args:
        log_level: Desired log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        json_format: If True, uses JSONFormatter; if False, uses standard text format.
        service_name: Name of the microservice or pipeline component.
        logger_name: Name of the target logger. If None, configures root logger.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(logger_name)
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Avoid duplicate handlers on re-initialization
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        if json_format:
            formatter: logging.Formatter = JSONFormatter(service_name=service_name)
        else:
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False
    return logger
