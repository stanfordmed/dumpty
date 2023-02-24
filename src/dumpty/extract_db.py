import threading
from typing import List, Set
from tinydb import JSONStorage, Query, TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb_serialization import SerializationMiddleware
from tinydb_serialization.serializers import DateTimeSerializer
from dumpty.extract import Extract

from dumpty.serializers import DecimalSerializer, FloatSerializer


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
        return self.extracts.get(table_name, Extract(table_name))
