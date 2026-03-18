"""Unit tests for dumpty.exceptions module."""

from dumpty.exceptions import ExtractError
from dumpty.exceptions import ValidationError
from dumpty.extract import Extract


class TestValidationError:
    def test_message(self):
        ex = ValidationError("table not found")
        assert "table not found" in str(ex)


class TestExtractError:
    def test_default_message(self):
        e = Extract("my_table")
        ex = ExtractError(e)
        assert "my_table" in ex.message

    def test_custom_message(self):
        e = Extract("my_table")
        ex = ExtractError(e, "Spark context lost")
        assert ex.message == "Spark context lost"
        assert ex.extract is e
