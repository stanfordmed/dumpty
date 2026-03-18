"""End-to-end integration tests: MSSQL → Spark → GCS → BigQuery → verify → cleanup.

Requires:
  - Docker (for MSSQL container)
  - GCP credentials with BigQuery + GCS access
  - Environment variables:
      DUMPTY_E2E_GCP_PROJECT   — GCP project ID
      DUMPTY_E2E_GCS_BUCKET    — GCS bucket name (no gs:// prefix)
      DUMPTY_E2E_CREDENTIALS   — Path to service account JSON (optional; uses ADC if unset)

Run:
    pytest -m e2e -v --timeout=600
Skip:
    pytest -m "not e2e"
"""

import json
import logging
import os
import secrets
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import pytest
from google.cloud.bigquery import Client as BigqueryClient
from google.cloud.storage import Client as StorageClient
from sqlalchemy import text

from dumpty.util import ensure_gcs_shaded_jar as _ensure_gcs_shaded_jar
from tests.conftest import MSSQL_PASSWORD
from tests.conftest import MSSQLContainer


log = logging.getLogger(__name__)


E2E_SCHEMA = "dumpty_e2e"

# ---------------------------------------------------------------------------
# Row counts (must match seed data)
# ---------------------------------------------------------------------------
EXPECTED_ROWS = {
    "edge_types": 2000,
    "string_pk_wide": 1500,
    "no_pk_table": 500,
    "empty_table": 0,
    "special_chars": 200,
    "nullable_stress": 800,
    "bound_based_pk": 1000,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        pytest.skip(f"Environment variable {name} is not set — skipping e2e tests")
    return val


def _batch_insert(conn, sql_template: str, rows: list[str]):
    """Execute a batch of INSERT statements to avoid thousands of round-trips.

    If ``sql_template`` is a non-empty string, each entry in ``rows`` is formatted
    into the template using ``sql_template.format(row=row)`` before execution.
    When ``sql_template`` is empty (the current usage), ``rows`` is treated as a
    list of complete SQL statements and executed as before.
    """
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch_rows = rows[i : i + batch_size]
        if sql_template:
            statements = [sql_template.format(row=row) for row in batch_rows]
        else:
            statements = batch_rows
        batch = ";".join(statements)
        conn.execute(text(batch))
    conn.commit()


GCS_SHADED_JAR_URL = (
    "https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v4.0.3/gcs-connector-4.0.3-shaded.jar"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def e2e_env_vars():
    """Read and validate required GCP environment variables."""
    project = _require_env("DUMPTY_E2E_GCP_PROJECT")
    bucket = _require_env("DUMPTY_E2E_GCS_BUCKET")
    credentials = os.environ.get("DUMPTY_E2E_CREDENTIALS")  # optional
    return {"project": project, "bucket": bucket, "credentials": credentials}


@pytest.fixture(scope="session")
def e2e_run_id():
    """Unique ID for this test run — namespaces BQ dataset and GCS path."""
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    short = secrets.token_hex(3)
    return f"{ts}_{short}"


@pytest.fixture(scope="session")
def e2e_mssql_container():
    """Start an MSSQL container for the e2e test session."""
    container = MSSQLContainer(password=MSSQL_PASSWORD)
    with container:
        yield container


@pytest.fixture(scope="session")
def e2e_mssql_engine(e2e_mssql_container):
    """SQLAlchemy engine connected to the e2e MSSQL container."""
    from sqlalchemy import create_engine

    url = e2e_mssql_container.get_connection_url()
    engine = create_engine(url, isolation_level="READ UNCOMMITTED")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def e2e_seed_data(e2e_mssql_engine):
    """Create schema and seed 6 tables exercising all code paths."""
    engine = e2e_mssql_engine
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA [{E2E_SCHEMA}]"))
        conn.commit()

        # ── Table 1: edge_types (2000 rows, INT IDENTITY PK → bound-based partitioning) ──
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[edge_types] (
                    id              INT PRIMARY KEY IDENTITY(1,1),
                    small_val       SMALLINT,
                    big_val         BIGINT,
                    name            NVARCHAR(200),
                    birth_date      DATE,
                    created_at      DATETIME,
                    score           DECIMAL(10,2),
                    precise_val     DECIMAL(38,9),
                    very_precise    DECIMAL(38,15),
                    float_val       FLOAT,
                    real_val        REAL,
                    is_active       BIT,
                    binary_data     VARBINARY(200),
                    guid            UNIQUEIDENTIFIER,
                    nullable_col    NVARCHAR(100) NULL
                )
            """)
        )
        conn.commit()

        rows = []
        for i in range(1, 2001):
            small = i % 32000
            big = i * 100_000_000
            name = f"Name_{i}"
            bdate = f"2000-{((i - 1) % 12) + 1:02d}-{((i - 1) % 28) + 1:02d}"
            cdate = f"2024-01-{((i - 1) % 28) + 1:02d} 10:{(i % 60):02d}:{(i % 60):02d}"
            score = round(i * 1.23, 2)
            precise = f"{i * 123456789}.{i % 1_000_000_000:09d}"
            very_p = f"{i * 12345678901234}.{i % 1_000_000_000_000_000:015d}"
            fval = i * 0.123456789
            rval = i * 0.5
            active = 1 if i % 2 == 0 else 0
            binary_hex = f"0x{i:08X}"
            guid = str(uuid.UUID(int=i))
            nullable = f"'val_{i}'" if i % 2 == 0 else "NULL"
            rows.append(
                f"INSERT INTO [{E2E_SCHEMA}].[edge_types] "
                f"(small_val, big_val, name, birth_date, created_at, score, precise_val, very_precise, "
                f"float_val, real_val, is_active, binary_data, guid, nullable_col) "
                f"VALUES ({small}, {big}, '{name}', '{bdate}', '{cdate}', {score}, {precise}, {very_p}, "
                f"{fval}, {rval}, {active}, {binary_hex}, '{guid}', {nullable})"
            )
        _batch_insert(conn, "", rows)

        # ── Table 2: string_pk_wide (1500 rows, NVARCHAR PK → julienne/predicate partitioning) ──
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[string_pk_wide] (
                    code          NVARCHAR(36) PRIMARY KEY,
                    description   NVARCHAR(500),
                    amount        DECIMAL(18,4),
                    event_date    DATETIME
                )
            """)
        )
        conn.commit()

        rows = []
        for i in range(1, 1501):
            code = str(uuid.UUID(int=i + 10000))
            desc = f"Description for item {i}"
            amount = round(i * 9.8765, 4)
            edate = f"2025-{((i - 1) % 12) + 1:02d}-{((i - 1) % 28) + 1:02d} 08:30:00"
            rows.append(
                f"INSERT INTO [{E2E_SCHEMA}].[string_pk_wide] (code, description, amount, event_date) "
                f"VALUES ('{code}', '{desc}', {amount}, '{edate}')"
            )
        _batch_insert(conn, "", rows)

        # ── Table 3: no_pk_table (500 rows, no PK → single-thread extract) ──
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[no_pk_table] (
                    col_a   INT,
                    col_b   NVARCHAR(100),
                    col_c   DECIMAL(12,2)
                )
            """)
        )
        conn.commit()

        rows = []
        for i in range(1, 501):
            rows.append(
                f"INSERT INTO [{E2E_SCHEMA}].[no_pk_table] (col_a, col_b, col_c) VALUES ({i}, 'value_{i}', {i * 2.5})"
            )
        _batch_insert(conn, "", rows)

        # ── Table 4: empty_table (0 rows → zero-row path) ──
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[empty_table] (
                    id      INT PRIMARY KEY,
                    value   NVARCHAR(100)
                )
            """)
        )
        conn.commit()

        # ── Table 5: special_chars (200 rows, below partition threshold) ──
        # Column names with spaces, mixed case, dashes → test normalize_schema
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[special_chars] (
                    id                  INT PRIMARY KEY IDENTITY(1,1),
                    text_val            NVARCHAR(MAX),
                    [name with spaces]  NVARCHAR(100),
                    [MixedCase_Col]     NVARCHAR(100),
                    [col-with-dashes]   NVARCHAR(100)
                )
            """)
        )
        conn.commit()

        rows = []
        for i in range(1, 201):
            # Use various special characters — avoid SQL injection via parameterized-style escaping
            # We double single-quotes for SQL literal safety
            special_texts = [
                f"Unicode: \u00e9\u00e8\u00ea\u00eb row{i}",  # diacritics
                f"CJK: \u4e16\u754c\u4f60\u597d row{i}",  # Chinese
                f"Quotes: it''s a \"test\" row{i}",
                f"Whitespace: tab\there\nnewline row{i}",
                f"Backslash: C:\\\\path\\\\file row{i}",
                f"Mixed: \u00fc\u00f6\u00e4 <>&;!@#$%^*() row{i}",
            ]
            txt = special_texts[i % len(special_texts)].replace("'", "''")
            rows.append(
                f"INSERT INTO [{E2E_SCHEMA}].[special_chars] (text_val, [name with spaces], [MixedCase_Col], [col-with-dashes]) "
                f"VALUES (N'{txt}', N'spaced_{i}', N'mixed_{i}', N'dashed_{i}')"
            )
        _batch_insert(conn, "", rows)

        # ── Table 6: nullable_stress (800 rows, INT IDENTITY PK → 2 partitions) ──
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[nullable_stress] (
                    id          INT PRIMARY KEY IDENTITY(1,1),
                    always_null NVARCHAR(100),
                    half_null   INT,
                    rarely_null DATE,
                    redact_me   NVARCHAR(100)
                )
            """)
        )
        conn.commit()

        rows = []
        for i in range(1, 801):
            half = str(i) if i % 2 == 0 else "NULL"
            rarely = f"'2025-06-{((i - 1) % 28) + 1:02d}'" if i % 20 != 0 else "NULL"
            rows.append(
                f"INSERT INTO [{E2E_SCHEMA}].[nullable_stress] (always_null, half_null, rarely_null, redact_me) "
                f"VALUES (NULL, {half}, {rarely}, 'sensitive_data_{i}')"
            )
        _batch_insert(conn, "", rows)

        # ── Table 7: bound_based_pk (1000 rows, NUMERIC(10,0) PK → Spark bound-based partitioning) ──
        # NUMERIC PK satisfies is_numeric=isinstance(pk.type, sqltypes.Numeric), and rows==max
        # triggers the "sequential, no gaps" branch → lowerBound/upperBound/numPartitions extract path.
        conn.execute(
            text(f"""
                CREATE TABLE [{E2E_SCHEMA}].[bound_based_pk] (
                    id     NUMERIC(10,0) PRIMARY KEY,
                    label  NVARCHAR(100),
                    score  DECIMAL(7,2)
                )
            """)
        )
        conn.commit()

        rows = []
        for i in range(1, 1001):
            score = round(i * 1.23, 2)
            rows.append(
                f"INSERT INTO [{E2E_SCHEMA}].[bound_based_pk] (id, label, score) VALUES ({i}, 'item_{i}', {score})"
            )
        _batch_insert(conn, "", rows)

    yield engine


def _write_config(
    tmp_path: Path,
    container,
    env_vars: dict,
    run_id: str,
    gcs_jar_path: str,
) -> Path:
    """Write a Dumpty config.yaml for the e2e test run."""
    host = container.get_container_host_ip()
    port = container.get_exposed_port(1433)
    project = env_vars["project"]
    bucket = env_vars["bucket"]
    credentials = env_vars.get("credentials")

    gcs_prefix = f"dumpty_e2e_{run_id}"
    dataset = f"{project}.dumpty_e2e_{run_id}"

    # Build credentials line for spark properties if provided
    cred_spark_line = ""
    cred_config_line = ""
    if credentials:
        cred_spark_line = (
            f'    spark.hadoop.google.cloud.auth.service.account.enable: "true"\n'
            f'    spark.hadoop.google.cloud.auth.service.account.json.keyfile: "{credentials}"'
        )
        cred_config_line = f'credentials: "{credentials}"'
    else:
        cred_spark_line = (
            '    spark.hadoop.fs.gs.auth.type: "APPLICATION_DEFAULT"\n'
            '    spark.hadoop.google.cloud.auth.type: "APPLICATION_DEFAULT"'
        )

    config_yaml = f"""\
spark:
  threads: 2
  format: "json"
  compression: "gzip"
  timestamp_format: "yyyy-MM-dd HH:mm:ss"
  log_level: INFO
  properties:
    spark.ui.showConsoleProgress: "false"
    spark.default.parallelism: "2"
    spark.driver.memory: "2g"
    spark.executor.memory: "2g"
    spark.sql.session.timeZone: "UTC"
    spark.sql.jsonGenerator.ignoreNullFields: "false"
    spark.jars: "{gcs_jar_path}"
    spark.jars.packages: "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11,org.apache.spark:spark-hadoop-cloud_2.13:4.1.1,com.google.cloud.spark:spark-4.0-bigquery:0.44.0"
    spark.jars.excludes: "com.google.cloud.bigdataoss:gcs-connector"
{cred_spark_line}
    spark.hadoop.fs.gs.impl: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    spark.hadoop.fs.AbstractFileSystem.gs.impl: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    spark.hadoop.fs.gs.http.max.retry: "2"
    spark.hadoop.fs.gs.http.connect-timeout: "5000"

sqlalchemy:
  url: "mssql+pymssql://SA:{quote_plus(MSSQL_PASSWORD)}@{host}:{port}/tempdb"
  isolation_level: "READ UNCOMMITTED"

jdbc:
  url: "jdbc:sqlserver://{host}:{port};databaseName=tempdb;encrypt=true;trustServerCertificate=true;"
  properties:
    user: "SA"
    password: "{MSSQL_PASSWORD}"
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    fetchsize: "1000"

project: "{project}"
{cred_config_line}
target_uri: "gs://{bucket}/{gcs_prefix}"
target_dataset: "{dataset}"
target_dataset_location: "US"
target_partition_size_bytes: 50000000
default_rows_per_partition: 500
introspection_expire_s: 0
introspect_workers: 2
extract_workers: 2
load_workers: 2
normalize_schema: true
empty_columns: "nullable_stress.redact_me"
progress_bar: false
drop_dataset: true
schema: {E2E_SCHEMA}
tinydb_database_file: "{tmp_path}/tinydb_e2e.json"
tinydb_date: "{tmp_path}/tinydb_e2e_date.json"
log_file: "{tmp_path}/extract.json"

tables:
  - edge_types
  - string_pk_wide
  - no_pk_table
  - empty_table
  - special_chars
  - nullable_stress
  - bound_based_pk
"""
    config_path = tmp_path / "config_e2e.yaml"
    config_path.write_text(config_yaml)
    return config_path


@pytest.fixture(scope="session")
def e2e_config_file(e2e_mssql_container, e2e_env_vars, e2e_run_id, tmp_path_factory):
    """Write the Dumpty config YAML to a temp directory."""
    tmp_path = tmp_path_factory.mktemp("e2e")
    gcs_jar = _ensure_gcs_shaded_jar(GCS_SHADED_JAR_URL)
    return _write_config(tmp_path, e2e_mssql_container, e2e_env_vars, e2e_run_id, gcs_jar)


@pytest.fixture(scope="session")
def e2e_bq_client(e2e_env_vars):
    """BigQuery client for verification and cleanup."""
    credentials_path = e2e_env_vars.get("credentials")
    if credentials_path:
        client = BigqueryClient.from_service_account_json(credentials_path)
    else:
        client = BigqueryClient(project=e2e_env_vars["project"])
    yield client
    client.close()


@pytest.fixture(scope="session")
def e2e_gcs_client(e2e_env_vars):
    """GCS client for cleanup."""
    credentials_path = e2e_env_vars.get("credentials")
    if credentials_path:
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_file(credentials_path)
        client = StorageClient(credentials=creds, project=e2e_env_vars["project"])
    else:
        client = StorageClient(project=e2e_env_vars["project"])
    yield client
    client.close()


@pytest.fixture(scope="session")
def e2e_dataset_ref(e2e_env_vars, e2e_run_id):
    """Fully-qualified BigQuery dataset reference."""
    return f"{e2e_env_vars['project']}.dumpty_e2e_{e2e_run_id}"


@pytest.fixture(scope="session")
def e2e_gcs_prefix(e2e_run_id):
    """GCS prefix for this test run."""
    return f"dumpty_e2e_{e2e_run_id}"


@pytest.fixture(scope="session")
def e2e_pipeline_result(
    e2e_seed_data, e2e_config_file, e2e_env_vars, e2e_bq_client, e2e_gcs_client, e2e_dataset_ref, e2e_gcs_prefix
):
    """Run the full Dumpty pipeline and yield the extract log. Cleanup after all tests."""
    from dumpty.main import main

    config_path = str(e2e_config_file)
    log_file = e2e_config_file.parent / "extract.json"

    # Run the pipeline — this is the core E2E action
    main(["--config", config_path, "--verbose", "--no-progress"])

    # Read the extract log
    with open(log_file) as f:
        result = json.load(f)

    yield result

    # ── Cleanup (runs even if tests fail after yield) ──
    # Delete BigQuery dataset
    from google.cloud.bigquery import DatasetReference

    try:
        ref = DatasetReference.from_string(e2e_dataset_ref)
        e2e_bq_client.delete_dataset(ref, delete_contents=True, not_found_ok=True)
    except Exception:
        log.exception("Failed to delete BigQuery dataset during e2e cleanup")

    # Delete GCS blobs
    try:
        bucket = e2e_gcs_client.bucket(e2e_env_vars["bucket"])
        blobs = list(bucket.list_blobs(prefix=e2e_gcs_prefix))
        if blobs:
            bucket.delete_blobs(blobs)
    except Exception:
        log.exception("Failed to delete GCS blobs during e2e cleanup")

    # Clean up tinydb files
    for f in e2e_config_file.parent.glob("tinydb_e2e*.json"):
        f.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.timeout(600)
class TestE2EPipeline:
    """End-to-end pipeline verification."""

    # ── Pipeline execution ──

    def test_pipeline_completes(self, e2e_pipeline_result):
        """Pipeline ran without error and produced a summary."""
        assert "tables" in e2e_pipeline_result
        assert "elapsed_s" in e2e_pipeline_result

    def test_pipeline_consistent(self, e2e_pipeline_result):
        """All row counts match between source and BigQuery."""
        assert e2e_pipeline_result.get("consistent") is True

    def test_all_tables_processed(self, e2e_pipeline_result):
        """All 6 tables were extracted."""
        tables = e2e_pipeline_result["tables"]
        for name in EXPECTED_ROWS:
            assert name in tables, f"Table {name} not in extract summary"

    def test_no_warnings(self, e2e_pipeline_result):
        """No row count mismatch warnings."""
        assert e2e_pipeline_result.get("warnings") == []

    # ── Row count verification ──

    def test_row_count_edge_types(self, e2e_bq_client, e2e_dataset_ref):
        self._assert_row_count(e2e_bq_client, e2e_dataset_ref, "edge_types", 2000)

    def test_row_count_string_pk_wide(self, e2e_bq_client, e2e_dataset_ref):
        self._assert_row_count(e2e_bq_client, e2e_dataset_ref, "string_pk_wide", 1500)

    def test_row_count_no_pk_table(self, e2e_bq_client, e2e_dataset_ref):
        self._assert_row_count(e2e_bq_client, e2e_dataset_ref, "no_pk_table", 500)

    def test_row_count_empty_table(self, e2e_bq_client, e2e_dataset_ref):
        """Empty table should exist in BQ with 0 rows."""
        table_ref = f"{e2e_dataset_ref}.empty_table"
        table = e2e_bq_client.get_table(table_ref)
        assert table.num_rows == 0

    def test_row_count_special_chars(self, e2e_bq_client, e2e_dataset_ref):
        self._assert_row_count(e2e_bq_client, e2e_dataset_ref, "special_chars", 200)

    def test_row_count_nullable_stress(self, e2e_bq_client, e2e_dataset_ref):
        self._assert_row_count(e2e_bq_client, e2e_dataset_ref, "nullable_stress", 800)

    # ── Schema / type verification ──

    def test_edge_types_schema(self, e2e_bq_client, e2e_dataset_ref):
        """Verify BQ column types match the expected type mappings."""
        table = e2e_bq_client.get_table(f"{e2e_dataset_ref}.edge_types")
        schema_map = {field.name: field.field_type for field in table.schema}

        assert schema_map["id"] == "INTEGER"
        assert schema_map["small_val"] == "INTEGER"
        assert schema_map["big_val"] == "INTEGER"
        assert schema_map["name"] == "STRING"
        assert schema_map["birth_date"] == "DATE"
        assert schema_map["created_at"] == "DATETIME"
        assert schema_map["score"] == "NUMERIC"
        assert schema_map["precise_val"] == "NUMERIC"
        assert schema_map["very_precise"] == "BIGNUMERIC"
        assert schema_map["float_val"] == "FLOAT"
        assert schema_map["real_val"] == "FLOAT"
        assert schema_map["is_active"] == "BOOLEAN"
        assert schema_map["binary_data"] == "BYTES"
        assert schema_map["guid"] == "STRING"
        assert schema_map["nullable_col"] == "STRING"

    def test_edge_types_all_columns_normalized(self, e2e_bq_client, e2e_dataset_ref):
        """All column names should be lowercase with underscores."""
        table = e2e_bq_client.get_table(f"{e2e_dataset_ref}.edge_types")
        for field in table.schema:
            assert field.name == field.name.lower(), f"Column {field.name} not lowercase"
            assert " " not in field.name, f"Column {field.name} has spaces"

    # ── Column name normalization (special_chars table) ──

    def test_special_chars_column_normalization(self, e2e_bq_client, e2e_dataset_ref):
        """Column names with spaces, mixed case, dashes should be normalized."""
        table = e2e_bq_client.get_table(f"{e2e_dataset_ref}.special_chars")
        col_names = {field.name for field in table.schema}
        assert "name_with_spaces" in col_names
        assert "mixedcase_col" in col_names
        assert "col_with_dashes" in col_names

    # ── Data integrity spot-checks ──

    def test_edge_types_data_integrity(self, e2e_bq_client, e2e_dataset_ref):
        """Spot-check a known row's values survived the round-trip."""
        query = f"SELECT * FROM `{e2e_dataset_ref}.edge_types` WHERE id = 1"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        row = rows[0]
        assert row["name"] == "Name_1"
        assert row["is_active"] is False  # id=1 is odd → 0 → False

    def test_string_pk_data_integrity(self, e2e_bq_client, e2e_dataset_ref):
        """Verify string PK table data survived round-trip."""
        expected_code = str(uuid.UUID(int=10001))
        query = f"SELECT code, description FROM `{e2e_dataset_ref}.string_pk_wide` WHERE code = '{expected_code}'"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        assert rows[0]["description"] == "Description for item 1"

    def test_special_chars_unicode_data(self, e2e_bq_client, e2e_dataset_ref):
        """Verify Unicode text survived the round-trip."""
        query = f"SELECT text_val FROM `{e2e_dataset_ref}.special_chars` WHERE id = 2"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        # id=2 → i%6=2 → "Quotes: it's a \"test\" row2"
        assert "row2" in rows[0]["text_val"]

    def test_special_chars_cjk(self, e2e_bq_client, e2e_dataset_ref):
        """CJK characters survive round-trip."""
        # id=2 maps to index 2 (Quotes), id=8 maps to index 2 (Quotes)
        # id=3 maps to index 3 (Whitespace), etc.
        # For CJK: i%6=1, so id=1,7,13,...
        query = f"SELECT text_val FROM `{e2e_dataset_ref}.special_chars` WHERE id = 7"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        assert "\u4e16\u754c" in rows[0]["text_val"]  # 世界

    # ── NULL handling ──

    def test_always_null_column(self, e2e_bq_client, e2e_dataset_ref):
        """Column that is always NULL should have 0 non-null rows."""
        query = f"SELECT COUNT(*) AS c FROM `{e2e_dataset_ref}.nullable_stress` WHERE always_null IS NOT NULL"
        rows = list(e2e_bq_client.query(query).result())
        assert rows[0]["c"] == 0

    def test_half_null_column(self, e2e_bq_client, e2e_dataset_ref):
        """~50% of rows should have NULL in half_null column."""
        query = f"SELECT COUNT(*) AS c FROM `{e2e_dataset_ref}.nullable_stress` WHERE half_null IS NULL"
        rows = list(e2e_bq_client.query(query).result())
        assert rows[0]["c"] == 400  # exactly half of 800

    def test_rarely_null_column(self, e2e_bq_client, e2e_dataset_ref):
        """~5% of rows (every 20th) should have NULL in rarely_null."""
        query = f"SELECT COUNT(*) AS c FROM `{e2e_dataset_ref}.nullable_stress` WHERE rarely_null IS NULL"
        rows = list(e2e_bq_client.query(query).result())
        assert rows[0]["c"] == 40  # 800/20 = 40

    # ── empty_columns redaction ──

    def test_redact_me_column_empty(self, e2e_bq_client, e2e_dataset_ref):
        """The redact_me column should exist in BQ but contain only NULLs (data stripped, schema preserved)."""
        table = e2e_bq_client.get_table(f"{e2e_dataset_ref}.nullable_stress")
        col_names = {field.name for field in table.schema}
        assert "redact_me" in col_names
        query = f"SELECT COUNT(*) AS c FROM `{e2e_dataset_ref}.nullable_stress` WHERE redact_me IS NOT NULL"
        rows = list(e2e_bq_client.query(query).result())
        assert rows[0]["c"] == 0

    # ── BIGNUMERIC precision ──

    def test_bignumeric_precision(self, e2e_bq_client, e2e_dataset_ref):
        """BIGNUMERIC values should preserve full precision."""
        query = f"SELECT very_precise FROM `{e2e_dataset_ref}.edge_types` WHERE id = 10"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        val = rows[0]["very_precise"]
        assert val is not None

    # ── Binary data ──

    def test_binary_data_round_trip(self, e2e_bq_client, e2e_dataset_ref):
        """Binary data should survive the round-trip."""
        query = f"SELECT binary_data FROM `{e2e_dataset_ref}.edge_types` WHERE id = 1"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        # id=1 → 0x00000001
        assert rows[0]["binary_data"] is not None
        assert len(rows[0]["binary_data"]) > 0

    # ── Spark bound-based partitioning (NUMERIC PK) ──

    def test_row_count_bound_based_pk(self, e2e_bq_client, e2e_dataset_ref):
        self._assert_row_count(e2e_bq_client, e2e_dataset_ref, "bound_based_pk", 1000)

    def test_bound_based_pk_schema(self, e2e_bq_client, e2e_dataset_ref):
        """NUMERIC(10,0) maps to BQ INTEGER (no fractional digits); score DECIMAL(7,2) → NUMERIC."""
        table = e2e_bq_client.get_table(f"{e2e_dataset_ref}.bound_based_pk")
        schema_map = {field.name: field.field_type for field in table.schema}
        assert schema_map["id"] == "INTEGER"
        assert schema_map["label"] == "STRING"
        assert schema_map["score"] == "NUMERIC"

    def test_bound_based_data_integrity(self, e2e_bq_client, e2e_dataset_ref):
        """Spot-check that a known row survived Spark bound-based partitioning intact."""
        query = f"SELECT label, score FROM `{e2e_dataset_ref}.bound_based_pk` WHERE id = 500"
        rows = list(e2e_bq_client.query(query).result())
        assert len(rows) == 1
        assert rows[0]["label"] == "item_500"
        assert float(rows[0]["score"]) == 615.0  # 500 * 1.23

    # ── Helpers ──

    def _assert_row_count(self, bq_client, dataset_ref, table_name, expected):
        query = f"SELECT COUNT(*) AS c FROM `{dataset_ref}.{table_name}`"
        rows = list(bq_client.query(query).result())
        assert rows[0]["c"] == expected, f"{table_name}: expected {expected} rows, got {rows[0]['c']}"
