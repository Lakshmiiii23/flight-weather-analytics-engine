"""Unit tests for JSON logging formatter and setup routines."""

import json
import logging
import io
import pytest
from config.logging_config import JSONFormatter, setup_logging


def test_json_formatter_standard_fields():
    """Verify standard fields are included in serialized log outputs."""
    formatter = JSONFormatter(service_name="test-service")
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=42,
        msg="Telemetry batch processed successfully",
        args=(),
        exc_info=None,
    )
    
    formatted_output = formatter.format(record)
    parsed = json.loads(formatted_output)

    assert parsed["service"] == "test-service"
    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test_logger"
    assert parsed["message"] == "Telemetry batch processed successfully"
    assert "timestamp" in parsed
    assert parsed["line"] == 42


def test_json_formatter_with_exception():
    """Verify exceptions are parsed into type, message, and traceback array."""
    formatter = JSONFormatter(service_name="test-service")
    try:
        raise ValueError("Rate limit exceeded on external API")
    except ValueError:
        import sys
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="error_logger",
        level=logging.ERROR,
        pathname=__file__,
        lineno=60,
        msg="API call failed",
        args=(),
        exc_info=exc_info,
    )

    formatted_output = formatter.format(record)
    parsed = json.loads(formatted_output)

    assert "exception" in parsed
    assert parsed["exception"]["type"] == "ValueError"
    assert "Rate limit exceeded" in parsed["exception"]["message"]
    assert len(parsed["exception"]["traceback"]) > 0


def test_json_formatter_with_context():
    """Verify extra context parameters are nested under the 'context' dictionary."""
    formatter = JSONFormatter(service_name="test-service")
    record = logging.LogRecord(
        name="metrics_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=80,
        msg="Partition evicted",
        args=(),
        exc_info=None,
    )
    record.__dict__["partition_date"] = "2026-10-01"
    record.__dict__["bytes_evicted"] = 1048576

    formatted_output = formatter.format(record)
    parsed = json.loads(formatted_output)

    assert "context" in parsed
    assert parsed["context"]["partition_date"] == "2026-10-01"
    assert parsed["context"]["bytes_evicted"] == 1048576


def test_setup_logging_singleton():
    """Verify setup_logging configures stream handler and correct log level."""
    logger_name = "unit_test_custom_logger"
    logger = setup_logging(log_level="DEBUG", json_format=True, logger_name=logger_name)

    assert logger.level == logging.DEBUG
    assert len(logger.handlers) >= 1
    assert isinstance(logger.handlers[0].formatter, JSONFormatter)
