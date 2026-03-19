"""Integration tests for dumpty.pipeline — requires a running MSSQL container.

Run with:  pytest -m integration
"""

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import MetaData
from sqlalchemy import Table
from tenacity import Retrying
from tenacity import stop_after_attempt

from dumpty.config import Config
from dumpty.config import JdbcConfig
from dumpty.config import SparkConfig
from dumpty.config import SqlalchemyConfig
from dumpty.extract import Extract
from dumpty.pipeline import Pipeline


TEST_SCHEMA = "dumpty_test"


def _make_config(mssql_engine) -> Config:
    """Build a Config pointing at the test MSSQL container."""
    url = str(mssql_engine.url)
    return Config(
        spark=SparkConfig(threads=2, properties={}),
        jdbc=JdbcConfig(url=url.replace("+pyodbc", ""), properties={}),
        sqlalchemy=SqlalchemyConfig(url=url, isolation_level="READ UNCOMMITTED"),
        schema=TEST_SCHEMA,
        tables=["patients", "encounters", "empty_table", "no_pk_table"],
        schemaonly=False,  # need full introspection for row counts
    )


@pytest.fixture()
def pipeline_config(mssql_setup):
    return _make_config(mssql_setup)


@pytest.fixture()
def retryer():
    return Retrying(stop=stop_after_attempt(1))


# ---------------------------------------------------------------------------
# BQ Schema generation (static, no container needed but tested with real table)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBqSchema:
    def test_schema_from_patients(self, mssql_setup, pipeline_config):
        metadata = MetaData(schema=TEST_SCHEMA)
        table = Table("patients", metadata, autoload_with=mssql_setup)
        schema = Pipeline.bq_schema(table)

        names = {s["name"] for s in schema}
        assert "patient_id" in names
        assert "first_name" in names
        assert "score" in names

        # Verify type mappings
        by_name = {s["name"]: s for s in schema}
        assert by_name["patient_id"]["type"] == "INT64"
        assert by_name["first_name"]["type"] == "STRING"
        assert by_name["dob"]["type"] == "DATE"
        assert by_name["score"]["type"] in ("NUMERIC", "INT64", "FLOAT64")

    def test_schema_normalizes_names(self, mssql_setup, pipeline_config):
        metadata = MetaData(schema=TEST_SCHEMA)
        table = Table("patients", metadata, autoload_with=mssql_setup)
        schema = Pipeline.bq_schema(table)
        for s in schema:
            assert s["name"] == s["name"].lower()
            assert " " not in s["name"]


# ---------------------------------------------------------------------------
# Introspection (requires MSSQL container)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIntrospect:
    @patch("dumpty.pipeline.GCP")
    def test_introspect_patients(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        extract = Extract("patients")
        result = pipeline.introspect(extract)

        assert result.rows == 100
        assert result.bq_schema is not None
        assert len(result.bq_schema) > 0
        assert result.introspect_date is not None

    @patch("dumpty.pipeline.GCP")
    def test_introspect_empty_table(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        extract = Extract("empty_table")
        result = pipeline.introspect(extract)

        assert result.rows == 0

    @patch("dumpty.pipeline.GCP")
    def test_introspect_no_pk_table(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        extract = Extract("no_pk_table")
        result = pipeline.introspect(extract)

        assert result.rows == 10
        assert result.partition_column is None

    @patch("dumpty.pipeline.GCP")
    def test_introspect_encounters_string_pk(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        extract = Extract("encounters")
        result = pipeline.introspect(extract)

        assert result.rows == 50
        assert result.bq_schema is not None


# ---------------------------------------------------------------------------
# Reconcile (requires MSSQL container)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestReconcile:
    @patch("dumpty.pipeline.GCP")
    def test_reconcile_valid_tables(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        pipeline.reconcile(["patients", "encounters"])  # should not raise

    @patch("dumpty.pipeline.GCP")
    def test_reconcile_invalid_table_raises(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        from dumpty.exceptions import ValidationError

        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        with pytest.raises(ValidationError, match="nonexistent_table"):
            pipeline.reconcile(["patients", "nonexistent_table"])


# ---------------------------------------------------------------------------
# DataFrame helpers (no container needed)
# ---------------------------------------------------------------------------


class TestDataFrameHelpers:
    def test_normalize_df(self):
        """Test normalize_df calls select with normalized column names."""
        mock_df = MagicMock()
        mock_df.columns = ["col_a", "col_b"]
        # col() requires SparkContext, so patch it
        with patch("dumpty.pipeline.col") as mock_col:
            mock_col.side_effect = lambda x: MagicMock(alias=MagicMock(return_value=x))
            Pipeline.normalize_df(mock_df)
            mock_df.select.assert_called_once()

    def test_empty_cols_no_match(self):
        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        result = Pipeline.empty_cols(mock_df, "other_table", "some_table.name")
        # No columns dropped because table name doesn't match
        assert result == mock_df

    def test_empty_cols_with_match(self):
        mock_df = MagicMock()
        mock_df.columns = ["id", "name", "ssn"]
        Pipeline.empty_cols(mock_df, "patients", "patients.ssn")
        mock_df.select.assert_called_once()


# ---------------------------------------------------------------------------
# UDT registration (requires MSSQL container with UDTs created in conftest)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestUdtRegistration:
    @patch("dumpty.pipeline.GCP")
    def test_udts_registered_in_ischema(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        Pipeline(mssql_setup, retryer, pipeline_config)
        ischema = mssql_setup.dialect.ischema_names
        assert "VDT_SERIALNUMBER" in ischema
        assert "VDT_DATETIME" in ischema
        assert "VDT_FLAG" in ischema
        assert "VDT_NAME" in ischema

    @patch("dumpty.pipeline.GCP")
    def test_udt_table_reflects_correct_types(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        Pipeline(mssql_setup, retryer, pipeline_config)
        metadata = MetaData(schema=TEST_SCHEMA)
        table = Table("udt_table", metadata, autoload_with=mssql_setup)
        schema = Pipeline.bq_schema(table)

        by_name = {s["name"]: s for s in schema}
        assert by_name["ser"]["type"] == "INT64"
        assert by_name["event_time"]["type"] == "DATETIME"
        assert by_name["active"]["type"] == "STRING"
        assert by_name["label"]["type"] == "STRING"

    @patch("dumpty.pipeline.GCP")
    def test_udt_introspect_succeeds(self, mock_gcp_cls, mssql_setup, pipeline_config, retryer):
        pipeline = Pipeline(mssql_setup, retryer, pipeline_config)
        extract = Extract("udt_table")
        result = pipeline.introspect(extract)
        assert result.rows == 1
        assert result.bq_schema is not None
        assert len(result.bq_schema) == 4
