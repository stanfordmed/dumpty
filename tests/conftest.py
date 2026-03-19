"""Shared pytest fixtures for dumpty tests."""

import platform
import time
from urllib.parse import quote_plus

import pymssql
import pytest
from sqlalchemy import create_engine
from sqlalchemy import text
from testcontainers.core.container import DockerContainer


# Azure SQL Edge for ARM64 (macOS Apple Silicon), standard image for x86_64
_IS_ARM = platform.machine() in ("arm64", "aarch64")
MSSQL_IMAGE = "mcr.microsoft.com/azure-sql-edge:latest" if _IS_ARM else "mcr.microsoft.com/mssql/server:2022-latest"
MSSQL_PASSWORD = "Dumpty_Test_P@ss1"  # noqa: S105 — test-only password
TEST_SCHEMA = "dumpty_test"


class MSSQLContainer(DockerContainer):
    """Lightweight MSSQL test container with pymssql-based readiness check.

    Avoids the deprecated @wait_container_is_ready decorator in testcontainers.mssql.
    Works with both mcr.microsoft.com/mssql/server and azure-sql-edge images.
    """

    def __init__(
        self,
        image: str = MSSQL_IMAGE,
        username: str = "SA",
        password: str = MSSQL_PASSWORD,
        port: int = 1433,
        dbname: str = "tempdb",
        **kwargs,
    ):
        super().__init__(image, **kwargs)
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.with_exposed_ports(self.port)
        self.with_env("ACCEPT_EULA", "Y")
        self.with_env("SA_PASSWORD", self.password)

    def _connect(self) -> None:
        port = int(self.get_exposed_port(self.port))
        host = self.get_container_host_ip()
        wait_secs = 120
        start = time.time()
        while time.time() - start < wait_secs:
            try:
                conn = pymssql.connect(
                    server=host, port=str(port), user=self.username, password=self.password, database=self.dbname
                )
                conn.cursor().execute("SELECT 1")
                conn.close()
                return
            except Exception:
                time.sleep(2)
        raise TimeoutError(f"MSSQL container not ready after {wait_secs}s")

    def start(self):
        super().start()
        self._connect()
        return self

    def get_connection_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        return f"mssql+pymssql://{self.username}:{quote_plus(self.password)}@{host}:{port}/{self.dbname}"


@pytest.fixture(scope="session")
def mssql_container():
    """Start an MSSQL container for the entire test session."""
    container = MSSQLContainer(image=MSSQL_IMAGE, password=MSSQL_PASSWORD)
    with container:
        yield container


@pytest.fixture(scope="session")
def mssql_engine(mssql_container):
    """Create a SQLAlchemy engine connected to the MSSQL container."""
    url = mssql_container.get_connection_url()
    engine = create_engine(url, isolation_level="READ UNCOMMITTED")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def mssql_setup(mssql_engine):
    """Create schema and seed tables once per session."""
    with mssql_engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA [{TEST_SCHEMA}]"))
        conn.commit()

        # Simple table with numeric PK (auto-increment)
        conn.execute(
            text(
                f"""
                CREATE TABLE [{TEST_SCHEMA}].[patients] (
                    patient_id INT PRIMARY KEY IDENTITY(1,1),
                    first_name NVARCHAR(100),
                    last_name NVARCHAR(100),
                    dob DATE,
                    score DECIMAL(10,2)
                )
                """
            )
        )
        conn.commit()

        # Insert enough rows to test partitioning logic
        for i in range(1, 101):
            conn.execute(
                text(
                    f"INSERT INTO [{TEST_SCHEMA}].[patients] (first_name, last_name, dob, score) "
                    f"VALUES ('First{i}', 'Last{i}', '1990-01-{(i % 28) + 1:02d}', {i * 1.5})"
                )
            )
        conn.commit()

        # Table with string PK (non-numeric, tests julienne path)
        conn.execute(
            text(
                f"""
                CREATE TABLE [{TEST_SCHEMA}].[encounters] (
                    encounter_id NVARCHAR(36) PRIMARY KEY,
                    patient_id INT,
                    encounter_type NVARCHAR(50),
                    encounter_date DATETIME
                )
                """
            )
        )
        conn.commit()

        for i in range(1, 51):
            conn.execute(
                text(
                    f"INSERT INTO [{TEST_SCHEMA}].[encounters] (encounter_id, patient_id, encounter_type, encounter_date) "
                    f"VALUES ('enc-{i:04d}', {(i % 100) + 1}, 'office_visit', '2024-06-{(i % 28) + 1:02d}')"
                )
            )
        conn.commit()

        # Empty table (tests zero-row path)
        conn.execute(
            text(
                f"""
                CREATE TABLE [{TEST_SCHEMA}].[empty_table] (
                    id INT PRIMARY KEY,
                    value NVARCHAR(100)
                )
                """
            )
        )
        conn.commit()

        # Table with no PK (tests single-thread extract path)
        conn.execute(
            text(
                f"""
                CREATE TABLE [{TEST_SCHEMA}].[no_pk_table] (
                    col_a INT,
                    col_b NVARCHAR(50)
                )
                """
            )
        )
        conn.commit()
        for i in range(1, 11):
            conn.execute(text(f"INSERT INTO [{TEST_SCHEMA}].[no_pk_table] (col_a, col_b) VALUES ({i}, 'val{i}')"))
        conn.commit()

        # User-defined types (UDTs) and a table that uses them
        conn.execute(text("CREATE TYPE [VDT_SERIALNUMBER] FROM BIGINT"))
        conn.execute(text("CREATE TYPE [VDT_DATETIME] FROM DATETIME"))
        conn.execute(text("CREATE TYPE [VDT_FLAG] FROM CHAR(1)"))
        conn.execute(text("CREATE TYPE [VDT_NAME] FROM NVARCHAR(64)"))
        conn.commit()

        conn.execute(
            text(
                f"""
                CREATE TABLE [{TEST_SCHEMA}].[udt_table] (
                    ser VDT_SERIALNUMBER NOT NULL PRIMARY KEY,
                    event_time VDT_DATETIME,
                    active VDT_FLAG,
                    label VDT_NAME
                )
                """
            )
        )
        conn.commit()
        conn.execute(
            text(
                f"INSERT INTO [{TEST_SCHEMA}].[udt_table] (ser, event_time, active, label) VALUES (1, '2026-01-01', 'Y', 'test')"
            )
        )
        conn.commit()

    yield mssql_engine
