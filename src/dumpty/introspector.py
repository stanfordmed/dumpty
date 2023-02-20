from dataclasses import dataclass, field
from datetime import datetime
from math import floor
from threading import Lock
from typing import List

from sqlalchemy import Column, MetaData, Table, func, inspect
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.orm import Session
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import *
from tinydb import JSONStorage, Query, TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb_serialization import SerializationMiddleware
from tinydb_serialization.serializers import DateTimeSerializer

from dumpty import logger
from dumpty.exceptions import ValidationException
from dumpty.util import normalize_str

lock = Lock()


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


class Introspector:
    """Class containing introspected table data for a single DB schema
    """

    def __enter__(self):
        """Handle the context manager open."""
        return self

    def __exit__(self, *_args):
        """Handle the context manager close."""
        self._db.close()

    def __init__(self, db: str, engine: Engine, schema: str = None):
        """Initialize a :class:`.Introspector` instance. 
           :param db: File name/path of TinyDB database eg. ``tables.json``
           :param engine: SqlAlchemy engine instance
           :param schema: Name of database schema containing these tables (default: ``None``)
        """
        serialization = SerializationMiddleware(JSONStorage)
        serialization.register_serializer(DateTimeSerializer(), 'TinyDate')
        self._schema = schema
        self._engine = engine
        self._metadata = MetaData(bind=engine, schema=schema)
        self._inspector: Inspector = inspect(engine)

        db: TinyDB = TinyDB(db, sort_keys=True,
                            indent=4, storage=CachingMiddleware(serialization))

        # Set the TinyDB table name to the schema
        db.default_table_name = schema if schema is not None else "_default"

        # Serialize TinyDB into list of Extract objects
        extracts: List[Extract] = {}
        for doc in db.all():
            extracts[doc['name']] = Extract(**doc)
        self._db = db
        self._extracts = extracts
        self._tables = {}

    def _julienne(self, table: Table, column: Column, width: int):
        logger.debug(
            f"Julienning {table.name} on {column.name} into {width}-row slices")
        with Session(self._engine) as session:
            subquery = session.query(column.label('id'),
                                     func.row_number().over(order_by=column).label('row_num')).subquery()
            # There doesn't appear to be a generic way to modulo in SQLAlchemy?
            if self._engine.dialect.name == "mssql":
                modulo_filter = (subquery.c.row_num % width)
            else:
                modulo_filter = (func.mod(subquery.c.row_num, width))
            query = session.query(subquery.c.id).filter(
                modulo_filter == 0)
            return [r[0] for r in query.all()]

    @staticmethod
    def _bq_schema(table: Table) -> list[dict]:
        """
        Returns Table schema in BigQuery JSON schema format
        """
        col: Column
        schemas = []
        for col in table.columns:
            schema = {}
            schema['name'] = normalize_str(col.name)
            schema['mode'] = "Nullable" if col.nullable else "Required"

            if isinstance(col.type, (SmallInteger, Integer, BigInteger)):
                schema['type'] = "INT64"
            elif isinstance(col.type, (DateTime)):
                schema['type'] = "DATETIME"
            elif isinstance(col.type, (Date)):
                schema['type'] = "DATE"
            elif isinstance(col.type, (Float, REAL)):
                schema['type'] = "FLOAT64"
            elif isinstance(col.type, (String)):
                schema['type'] = "STRING"
            elif isinstance(col.type, (Boolean)):
                schema['type'] = "BOOL"
            elif isinstance(col.type, (LargeBinary)):
                schema['type'] = "BYTES"
            elif isinstance(col.type, (Numeric)):
                p = col.type.precision
                s = col.type.scale
                if s == 0 and p <= 18:
                    schema['type'] = "INT64"
                elif (s >= 0 and s <= 9) and ((max(s, 1) <= p) and p <= s+29):
                    schema['type'] = "NUMERIC"
                    schema['precision'] = p
                    schema['scale'] = s
                elif (s >= 0 and s <= 38) and ((max(s, 1) <= p) and p <= s+38):
                    schema['type'] = "BIGNUMERIC"
                    schema['precision'] = p
                    schema['scale'] = s
            else:
                logger.warning(
                    f"Unmapped type in {table.name}.{col.name} ({col.type}), defaulting to STRING")
                schema['type'] = "STRING"

            schemas.append(schema)
        return schemas

    def _introspect(self, extract: Extract, partitioning_threshold: int):

        logger.info(
            f"Introspecting {self._schema + '.' if self._schema is not None else ''}{extract.name}")
        # Introspect table from SQL database
        table = Table(extract.name, self._metadata, autoload=True)

        # Find min/max of PK (if it exists) and total row count
        pk = table.primary_key.columns[0] if table.primary_key else None
        with Session(self._engine) as session:
            if pk is not None:
                is_numeric = isinstance(pk.type, sqltypes.Numeric)
                if is_numeric:
                    qry = session.query(func.max(pk).label("max"),
                                        func.min(pk).label("min"),
                                        func.count().label("count")).select_from(table)
                    res = qry.one()
                    extract.max = int(res.max)
                    extract.min = int(res.min)
                    extract.rows = int(res.count)
                else:
                    qry = session.query(func.count().label("count"))
                    extract.rows = int(qry.scalar()).select_from(table)
            else:
                # If no PK we'll just dump the entire table in a single thread
                qry = session.query(func.count().label(
                    "count")).select_from(table)
                extract.rows = int(qry.scalar())

        # Only partition tables with a PK and rows exceeding partition_row_min
        if extract.rows >= partitioning_threshold and pk is not None:

            # Default to ~1M rows per partition
            if extract.partitions is None:
                extract.partitions = round(extract.rows / 1e6)

            if extract.partitions > 1:
                slice_width = floor(extract.rows / extract.partitions)
                if is_numeric:
                    if (extract.rows == extract.max) or (extract.rows == extract.max - 1) or (abs(extract.rows - (extract.max - extract.min)) <= 1):
                        # Numeric, sequential PK with no gaps uses default Spark column partitioning
                        extract.partition_column = pk.name
                    else:
                        # Numeric but PK is not sequential and likely heavily skewed, julienne the table instead
                        intervals = self._julienne(table, pk, slice_width)
                        i: int = 0
                        slices = []
                        while i <= len(intervals):
                            if i == 0:
                                slices.append(
                                    f"{pk.name} <= {intervals[i]} OR {pk.name} IS NULL ")
                            elif i == len(intervals):
                                slices.append(f"{pk.name} > {intervals[i-1]}")
                            else:
                                slices.append(
                                    f"{pk.name} > {intervals[i-1]} AND {pk.name} <= {intervals[i]}")
                            i += 1
                        extract.predicates = slices
                else:
                    # Non-numeric (varchar, datetime) so julienne the table since Spark partition_column must be numeric
                    intervals = self._julienne(table, pk, slice_width)
                    i: int = 0
                    slices = []
                    while i <= len(intervals):
                        if i == 0:
                            slices.append(
                                f"{pk.name} <= '{intervals[i]}' OR {pk.name} IS NULL ")
                        elif i == len(intervals):
                            slices.append(f"{pk.name} > '{intervals[i-1]}'")
                        else:
                            slices.append(
                                f"{pk.name} > '{intervals[i-1]}' AND {pk.name} <= '{intervals[i]}'")
                        i += 1
                    extract.predicates = slices
            else:
                # Recommendation from last run was to not split this table, remove old entries
                extract.partition_column = None
                extract.predicates = None
        else:
            extract.partitions = 0
            extract.predicates = None

        # Regenerate BQ Schema if needed
        extract.bq_schema = self._bq_schema(table)
        extract.introspect_date = datetime.now()

        return extract

    def introspect(self, table_name: str, expire_seconds: int = None, partitioning_threshold: int = 1e6) -> Extract:
        """Introspects (if needed) a table returning an Extract instance
           :param table_name: name of table to extract
           :param expire_seconds: will trigger re-introspection if expired
           :param partitioning_threshold: do not partition tables with less than this many rows (default: ``1,000,000``
        """
        extract: Extract = self._extracts.get(table_name)
        if extract is None:
            extract = Extract(table_name)
            self._introspect(extract, partitioning_threshold)
        else:
            if extract.introspect_date:
                duration = extract.introspect_date - datetime.now()
                if duration.total_seconds() > expire_seconds:
                    self._introspect(extract, partitioning_threshold)
            else:
                self._introspect(extract, partitioning_threshold)
        return extract

    def save(self, extract: Extract):
        """Saves an extract to the TinyDB database
           :param extract: extract instance to save to TinyDB
        """
        with lock:
            self._db.upsert(extract.__dict__, Query().name == extract.name)

    def reconcile(self, table_names: List[str]):
        """Checks if a list of table names exist in TinyDB and SQL database
            :param table_names: List of table names to validate against database
            :raises: :class:`.ValidationException` when a table is not found
        """
        logger.info(f"Enumerating tables in schema {self._schema}")
        sql_tables = self._inspector.get_table_names(schema=self._schema)
        not_found = [t for t in table_names if t not in sql_tables]
        if len(not_found) > 0:
            raise ValidationException(
                f"Could not find these tables in {self._schema}: {','.join(not_found)}")

    def close_tinydb(self):
        """Closes TinyDB (required if this class was not used as a context manager)
        """
        self._db.close()
