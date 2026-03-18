"""Unit tests for dumpty.config module."""

from dumpty.config import Config
from dumpty.config import JdbcConfig
from dumpty.config import SparkConfig
from dumpty.config import SqlalchemyConfig


class TestSqlalchemyConfig:
    def test_defaults(self):
        c = SqlalchemyConfig(url="mssql+pyodbc://localhost")
        assert c.isolation_level == "REPEATABLE READ"
        assert c.connect_args is None

    def test_custom_values(self):
        c = SqlalchemyConfig(
            url="mssql+pyodbc://localhost",
            connect_args={"TrustServerCertificate": "yes"},
            isolation_level="READ UNCOMMITTED",
        )
        assert c.connect_args is not None and c.connect_args["TrustServerCertificate"] == "yes"


class TestSparkConfig:
    def test_defaults(self):
        c = SparkConfig(threads=4, properties={})
        assert c.format == "json"
        assert c.compression == "gzip"
        assert c.log_level == "WARN"
        assert c.timestamp_format == "yyyy-MM-dd HH:mm:ss"


class TestConfig:
    def test_minimal_config(self):
        c = Config(
            spark=SparkConfig(threads=4, properties={}),
            jdbc=JdbcConfig(url="jdbc:sqlserver://localhost", properties={}),
            sqlalchemy=SqlalchemyConfig(url="mssql+pyodbc://localhost"),
            schema="dbo",
            tables=["table1", "table2"],
        )
        assert c.schema == "dbo"
        assert len(c.tables) == 2
        assert c.drop_dataset is False
        assert c.introspect_workers == 8

    def test_default_values(self):
        c = Config(
            spark=SparkConfig(threads=1, properties={}),
            jdbc=JdbcConfig(url="", properties={}),
            sqlalchemy=SqlalchemyConfig(url=""),
            schema="test",
            tables=[],
        )
        assert c.default_rows_per_partition == 1_000_000
        assert c.target_partition_size_bytes == 52428800
        assert c.normalize_schema is True
        assert c.retry is False
        assert c.progress_bar is True

    def test_from_yaml_string(self):
        yaml_str = """
spark:
    threads: 8
    properties: {}
jdbc:
    url: "jdbc:sqlserver://localhost"
    properties: {}
sqlalchemy:
    url: "mssql+pyodbc://localhost"
schema: dbo
tables:
    - patients
    - encounters
"""
        configs = Config.from_yaml(yaml_str)
        c = configs[0] if isinstance(configs, list) else configs
        assert c.spark.threads == 8
        assert c.schema == "dbo"
        assert "patients" in c.tables
