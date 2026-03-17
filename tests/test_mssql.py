"""Integration tests for MSSQL connectivity via SQLAlchemy — requires a running MSSQL container."""

import pytest
from sqlalchemy import inspect
from sqlalchemy import text


TEST_SCHEMA = "dumpty_test"


@pytest.mark.integration
class TestMSSQLConnection:
    def test_engine_connects(self, mssql_setup):
        with mssql_setup.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1

    def test_schema_exists(self, mssql_setup):
        inspector = inspect(mssql_setup)
        schemas = inspector.get_schema_names()
        assert TEST_SCHEMA in schemas

    def test_tables_exist(self, mssql_setup):
        inspector = inspect(mssql_setup)
        tables = inspector.get_table_names(schema=TEST_SCHEMA)
        assert "patients" in tables
        assert "encounters" in tables
        assert "empty_table" in tables
        assert "no_pk_table" in tables

    def test_row_counts(self, mssql_setup):
        with mssql_setup.connect() as conn:
            patients = conn.execute(text(f"SELECT COUNT(*) FROM [{TEST_SCHEMA}].[patients]")).scalar()
            encounters = conn.execute(text(f"SELECT COUNT(*) FROM [{TEST_SCHEMA}].[encounters]")).scalar()
            empty = conn.execute(text(f"SELECT COUNT(*) FROM [{TEST_SCHEMA}].[empty_table]")).scalar()
            assert patients == 100
            assert encounters == 50
            assert empty == 0

    def test_count_big_function(self, mssql_setup):
        """Verify COUNT_BIG works (important for tables > INT_MAX rows)."""
        with mssql_setup.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT_BIG(*) FROM [{TEST_SCHEMA}].[patients]")).scalar()
            assert result == 100

    def test_primary_key_introspection(self, mssql_setup):
        inspector = inspect(mssql_setup)
        pk = inspector.get_pk_constraint(table_name="patients", schema=TEST_SCHEMA)
        assert "patient_id" in pk["constrained_columns"]

        pk2 = inspector.get_pk_constraint(table_name="no_pk_table", schema=TEST_SCHEMA)
        assert pk2["constrained_columns"] == []
