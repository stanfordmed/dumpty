"""Unit tests for dumpty.util module."""

from sqlalchemy import literal_column
from sqlalchemy import select

from dumpty.util import CountBig
from dumpty.util import normalize_str


class TestNormalizeStr:
    def test_lowercase(self):
        assert normalize_str("Hello") == "hello"

    def test_spaces_replaced(self):
        assert normalize_str("first name") == "first_name"

    def test_special_chars_replaced(self):
        assert normalize_str("col-name.with@chars") == "col_name_with_chars"

    def test_already_normalized(self):
        assert normalize_str("simple_col_123") == "simple_col_123"

    def test_empty_string(self):
        assert normalize_str("") == ""

    def test_all_special(self):
        assert normalize_str("@#$%") == "____"

    def test_mixed_case_with_numbers(self):
        assert normalize_str("Patient_ID_2024") == "patient_id_2024"


class TestCountBig:
    def test_compiles_with_no_args(self):
        stmt = select(CountBig())
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "count_big" in compiled.lower()

    def test_compiles_with_expression(self):
        stmt = select(CountBig(literal_column("id")))
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "count_big" in compiled.lower()
        assert "id" in compiled
