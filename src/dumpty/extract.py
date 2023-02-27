
from dataclasses import field
import datetime
from typing import List
from dataclasses import dataclass


@dataclass
class Extract:
    """An extract of a table. This class is serialized as JSON in TinyDB so no complex data types."""
    name: str
    min: str = None
    max: str = None
    rows: int = 0
    introspect_date: datetime = None
    partition_column: str = None
    predicates: List[str] = field(default_factory=list)
    extract_uri: str = None
    extract_date: datetime = None
    partitions: int = None
    rows_loaded: int = 0
    bq_bytes: int = 0
    gcs_bytes: int = 0
    bq_schema: List[dict] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def consistent(self) -> bool:
        """Returns true if the row count in SQL server matches the rows loaded in BigQuery"""
        return self.rows == self.rows_loaded
