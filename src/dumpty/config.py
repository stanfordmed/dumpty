from dataclasses import dataclass
from dataclass_wizard import YAMLWizard
from typing import List, Dict


@dataclass
class SqlalchemyConfig:
    url: str
    pool_size: 20
    max_overflow: int = -1
    connect_args: dict = None


@ dataclass
class SparkConfig:
    threads: int
    properties: Dict
    format: str = "json"
    compression: str = "gzip"
    normalize_schema: bool = True
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    log_level: str = 'WARN'  # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


@ dataclass
class JdbcConfig:
    url: str
    properties: Dict


@ dataclass
class Config(YAMLWizard):
    spark: SparkConfig
    jdbc: JdbcConfig
    sqlalchemy: SqlalchemyConfig
    schema: str
    tables: List[str]
    target_uri: str = None
    target_dataset: str = None
    target_partition_size_bytes: int = 52428800
    introspection_expire_s: int = 0  # 0 = no expiration
    drop_dataset: bool = False
    progress_bar: bool = True
    log_file: str = 'extract.json'
    project: str = None
    credentials: str = None
    max_job_queue: int = 32
    retry: bool = False
    job_threads: int = 4
