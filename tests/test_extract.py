"""Unit tests for dumpty.extract module."""

from datetime import datetime
from decimal import Decimal

import pytest

from dumpty.extract import DecimalSerializer
from dumpty.extract import Extract
from dumpty.extract import ExtractDB
from dumpty.extract import FloatSerializer


class TestExtract:
    def test_default_values(self):
        e = Extract("test_table")
        assert e.name == "test_table"
        assert e.rows is None
        assert e.rows_loaded is None
        assert e.partitions is None
        assert e.bq_schema == []
        assert e.warnings == []

    def test_consistent_when_equal(self):
        e = Extract("t", rows=100, rows_loaded=100)
        assert e.consistent() is True

    def test_not_consistent_when_different(self):
        e = Extract("t", rows=100, rows_loaded=99)
        assert e.consistent() is False

    def test_not_consistent_when_none(self):
        e = Extract("t", rows=100, rows_loaded=None)
        assert e.consistent() is False

    def test_consistent_when_both_zero(self):
        """rows=0 and rows_loaded=0 must be consistent (empty table/view loaded correctly)."""
        e = Extract("t", rows=0, rows_loaded=0)
        assert e.consistent() is True

    def test_consistent_when_rows_none(self):
        """rows=None means the count was intentionally skipped (e.g. vv_ views); consistency check is bypassed."""
        e = Extract("t", rows=None, rows_loaded=0)
        assert e.consistent() is True

    def test_predicates_default_none(self):
        e = Extract("t")
        assert e.predicates is None

    def test_full_initialization(self):
        now = datetime.now()
        e = Extract(
            name="patients",
            min=1,
            max=1000,
            rows=1000,
            introspect_date=now,
            partition_column="patient_id",
            predicates=["patient_id <= 500", "patient_id > 500"],
            partitions=2,
        )
        assert e.partitions == 2
        assert e.predicates is not None and len(e.predicates) == 2
        assert e.partition_column == "patient_id"


class TestDecimalSerializer:
    def test_encode(self):
        s = DecimalSerializer()
        assert s.encode(Decimal("123.45")) == "123.45"

    def test_decode(self):
        s = DecimalSerializer()
        assert s.decode("123.45") == Decimal("123.45")

    def test_roundtrip(self):
        s = DecimalSerializer()
        val = Decimal("999.999")
        assert s.decode(s.encode(val)) == val


class TestFloatSerializer:
    def test_encode(self):
        s = FloatSerializer()
        assert s.encode(3.14) == "3.14"

    def test_decode(self):
        s = FloatSerializer()
        assert s.decode("3.14") == pytest.approx(3.14)

    def test_roundtrip(self):
        s = FloatSerializer()
        val = 2.71828
        assert s.decode(s.encode(val)) == pytest.approx(val)


class TestExtractDB:
    def test_context_manager_and_save_get(self, tmp_path):
        db_file = str(tmp_path / "test_db.json")
        with ExtractDB(db_file, "test_schema") as db:
            e = Extract("my_table", rows=42)
            db.save(e)

        # Re-open and verify persistence
        with ExtractDB(db_file, "test_schema") as db:
            loaded = db.get("my_table")
            assert loaded.name == "my_table"
            assert loaded.rows == 42

    def test_get_returns_new_extract_if_missing(self, tmp_path):
        db_file = str(tmp_path / "test_db.json")
        with ExtractDB(db_file) as db:
            e = db.get("nonexistent")
            assert e.name == "nonexistent"
            assert e.rows is None

    def test_upsert_updates_existing(self, tmp_path):
        db_file = str(tmp_path / "test_db.json")
        with ExtractDB(db_file, "test_schema") as db:
            e = Extract("tbl", rows=10)
            db.save(e)
            e.rows = 20
            db.save(e)

        with ExtractDB(db_file, "test_schema") as db:
            loaded = db.get("tbl")
            assert loaded.rows == 20
