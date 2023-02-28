import datetime
import threading
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Set

from tinydb import JSONStorage, Query, TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb_serialization import SerializationMiddleware, Serializer
from tinydb_serialization.serializers import DateTimeSerializer


@dataclass
class Extract:
    """This class represents the state of an Extract as it flows through the ELT pipeline. 
    This class is serialized as JSON in TinyDB so no complex data types.
    """
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


class DecimalSerializer(Serializer):
    OBJ_CLASS = Decimal

    def encode(self, obj):
        return str(obj)

    def decode(self, s):
        return Decimal(s)


class FloatSerializer(Serializer):
    OBJ_CLASS = float

    def encode(self, obj):
        return str(obj)

    def decode(self, s):
        return float(s)


class ExtractDB:
    """Creates a TinyDB-backed persistant database of Extract histories 
    """

    def __init__(self, db_file: str, default_table_name: str = None):
        self._db_file = db_file
        self._default_table_name = default_table_name
        self.mutex = threading.Lock()

    def __enter__(self):
        serialization = SerializationMiddleware(JSONStorage)
        serialization.register_serializer(DateTimeSerializer(), 'TinyDate')
        serialization.register_serializer(DecimalSerializer(), 'TinyDecimal')
        serialization.register_serializer(FloatSerializer(), 'TinyFloat')
        self._db: TinyDB = TinyDB(self._db_file, sort_keys=True,
                                  indent=4, storage=CachingMiddleware(serialization))
        # Set the TinyDB table name to the schema
        self._db.default_table_name = self._default_table_name if self._default_table_name is not None else "_default"

        self.extracts: Set[Extract] = {}
        for doc in self._db.all():
            self.extracts[doc['name']] = Extract(**doc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.close()

    def save(self, extract: Extract):
        """Saves an extract to the TinyDB database
           :param extract: extract instance to save to TinyDB
        """
        with self.mutex:
            self._db.upsert(extract.__dict__, Query().name == extract.name)

    def get(self, table_name: str) -> Extract:
        """Finds an Extract in TinyDB with the name :table_name:

        Args:
            table_name (str): Name of table to retrieve from TinyDB

        Returns:
            Extract: Existing Extract from TinyDB, or new Extract if none found
        """
        return self.extracts.get(table_name, Extract(table_name))
