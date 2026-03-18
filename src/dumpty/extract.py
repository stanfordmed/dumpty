import threading
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from decimal import Decimal

from funcy import omit
from tinydb import JSONStorage
from tinydb import Query
from tinydb import TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb_serialization import SerializationMiddleware
from tinydb_serialization import Serializer
from tinydb_serialization.serializers import DateTimeSerializer


@dataclass
class Extract:
    """This class represents the state of an Extract as it flows through the ELT pipeline.
    This class is serialized as JSON in TinyDB so no complex data types.
    """

    name: str
    min: int | None = None
    max: int | None = None
    rows: int | None = None
    introspect_date: datetime | None = None
    refresh_date: datetime | None = None
    partition_column: str | None = None
    predicates: list[str] | None = None
    extract_uri: str | None = None
    extract_date: datetime | None = None
    partitions: int | None = None
    rows_loaded: int | None = None
    bq_bytes: int | None = None
    gcs_bytes: int | None = None
    bq_schema: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

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
    """Creates a TinyDB-backed persistent database of Extract histories"""

    def __init__(self, db_file: str, default_table_name: str | None = None):
        self._db_file = db_file
        self._default_table_name = default_table_name
        self.mutex = threading.Lock()

    def __enter__(self):
        serialization = SerializationMiddleware(JSONStorage)
        serialization.register_serializer(DateTimeSerializer(), "TinyDate")
        serialization.register_serializer(DecimalSerializer(), "TinyDecimal")
        serialization.register_serializer(FloatSerializer(), "TinyFloat")
        self._db: TinyDB = TinyDB(self._db_file, sort_keys=True, indent=4, storage=CachingMiddleware(serialization))
        # Set the TinyDB table name to the schema
        self._db.default_table_name = self._default_table_name if self._default_table_name is not None else "_default"

        self.extracts: dict[str, Extract] = {}
        for doc in self._db.all():
            self.extracts[doc["name"]] = Extract(**doc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.close()

    def save(self, extract: Extract):
        """Saves an extract to the TinyDB database (excludes a few keys)
        :param extract: extract instance to save to TinyDB
        """
        to_save = dict(omit(extract.__dict__, "bq_schema"))
        with self.mutex:
            self._db.upsert(to_save, Query().name == extract.name)

    def get(self, table_name: str) -> Extract:
        """Finds an Extract in TinyDB with the name :table_name:

        Args:
            table_name (str): Name of table to retrieve from TinyDB

        Returns:
            Extract: Existing Extract from TinyDB, or new Extract if none found
        """
        return self.extracts.get(table_name, Extract(table_name))
