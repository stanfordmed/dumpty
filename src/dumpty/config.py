from dataclasses import dataclass, field
from typing import Dict, List

from dataclass_wizard import YAMLWizard


@dataclass
class SqlalchemyConfig:
    url: str
    connect_args: dict = None


@dataclass
class SparkConfig:
    threads: int
    properties: Dict
    format: str = "json"
    compression: str = "gzip"
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
    log_level: str = 'WARN'  # ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


@dataclass
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
    target_dataset_description: str = None
    target_dataset_location: str = "US"
    # Labels applied to the dataset at creation time, before extract
    target_dataset_pre_labels: dict = field(default_factory=dict)
    # Labels applied to dataset upon /successful/ extract completion
    target_dataset_post_labels: dict = field(default_factory=dict)
    target_dataset_access_entries: List[dict] = field(default_factory=list)
    target_dataset_additional_access_entries: List[dict] = field(
        default_factory=list)
    target_partition_size_bytes: int = 52428800
    introspection_expire_s: int = 0  # 0 = no expiration
    drop_dataset: bool = False
    progress_bar: bool = True
    log_file: str = 'extract.json'
    project: str = None
    credentials: str = None
    retry: bool = False
    tinydb_database_file: str = "tinydb.json"
    reconcile: bool = False
    normalize_schema: bool = True
    default_rows_per_partition: int = 1e6
    introspect_workers: int = 8
    extract_workers: int = 8
    load_workers: int = 32
