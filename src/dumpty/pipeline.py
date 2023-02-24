import json
from dataclasses import dataclass, field
from datetime import datetime
from math import ceil, floor
from queue import Queue
from threading import Lock, Thread
from typing import Callable, List

from sqlalchemy import Column, MetaData, Table, func, inspect
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.orm import Session
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import *
from tenacity import Retrying

from dumpty import logger
from dumpty.config import Config
from dumpty.exceptions import ValidationException
from dumpty.gcp import bigquery_load, get_size_bytes, upload_from_string
from dumpty.sql import bq_schema
from dumpty.util import normalize_str
from dumpty.worker import Worker
from dumpty.extract import Extract

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from dumpty import logger
from dumpty.config import Config
from dumpty.util import normalize_str


class Pipeline:
    """Wrapper class for the various stages of the ELT process
    """

    def __init__(self, engine: Engine, retryer: Retrying, config: Config):
        """Initialize a :class:`.Pipeline` instance. 
           :param spark: Spark instance
           :param engine: SqlAlchemy engine
           :param retryer: Retrying instance
           :param config: Config instance
        """
        self.config = config
        self.engine = engine
        self.retryer = retryer
        self._metadata = MetaData(bind=engine, schema=config.schema)
        self._inspector: Inspector = inspect(engine)

        self.introspect_queue = Queue(config.introspect_threads)
        self.extract_queue = Queue(config.extract_threads)
        self.load_queue = Queue(config.load_threads)
        self.done_queue = Queue()

        for _ in range(self.introspect_queue.maxsize):
            Worker(self.introspect, self.introspect_queue, self.extract_queue)
        for _ in range(self.extract_queue.maxsize):
            Worker(self.extract, self.extract_queue, self.load_queue)
        for _ in range(self.load_queue.maxsize):
            Worker(self.load, self.load_queue, self.done_queue)

    def __enter__(self):
        ctx = SparkSession\
            .builder\
            .master(f'local[{self.config.spark.threads}]')\
            .appName('Dumpty')\
            .config(conf=SparkConf().setAll(list(self.config.spark.properties.items())))
        self._spark_session = ctx.getOrCreate()
        self._spark_session.sparkContext.setLogLevel(
            self.config.spark.log_level)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._spark_session.stop()

    def submit(self, extract: Extract):
        self.introspect_queue.put(extract)

    @staticmethod
    def normalize_df(df: DataFrame) -> DataFrame:
        return df.select([col(x).alias(normalize_str(x)) for x in df.columns])

    def _julienne(self, table: Table, column: Column, width: int):
        logger.info(
            f"Julienning {table.name} on {column.name} into {width}-row slices")
        with Session(self.engine) as session:
            subquery = session\
                .query(column.label('id'),
                       func.row_number().over(order_by=column).label('row_num'))\
                .subquery()
            # There doesn't appear to be a generic way to modulo in SQLAlchemy?
            if self.engine.dialect.name == "mssql":
                modulo_filter = (subquery.c.row_num % width)
            else:
                modulo_filter = (func.mod(subquery.c.row_num, width))
            query = session\
                .query(subquery.c.id)\
                .filter(modulo_filter == 0)\
                .order_by(subquery.c.id)
            return [r[0] for r in query.all()]

    def _introspect(self, extract: Extract):
        logger.info(
            f"Introspecting {self.config.schema + '.' if self.config.schema is not None else ''}{extract.name}")
        # Introspect table from SQL database
        table = Table(extract.name, self._metadata, autoload=True)

        # Find min/max of PK (if it exists) and total row count
        pk = table.primary_key.columns[0] if table.primary_key else None
        with Session(self.engine) as session:
            if pk is not None:
                is_numeric = isinstance(pk.type, sqltypes.Numeric)
                if is_numeric:
                    qry = session.query(func.max(pk).label("max"),
                                        func.min(pk).label("min"),
                                        func.count().label("count")).select_from(table)
                    res = qry.one()
                    extract.max = res.max
                    extract.min = res.min
                    extract.rows = res.count
                else:
                    qry = session.query(func.count().label(
                        "count")).select_from(table)
                    extract.rows = qry.scalar()
            else:
                # If no PK we'll just dump the entire table in a single thread
                qry = session.query(func.count().label(
                    "count")).select_from(table)
                extract.rows = qry.scalar()

        # Only partition tables with a PK and rows exceeding partition_row_min
        if extract.rows >= self.config.partitioning_threshold and pk is not None:

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
        extract.bq_schema = bq_schema(table)
        extract.introspect_date = datetime.now()

        return extract

    def introspect(self, extract: Extract) -> Extract:
        """Introspects (if needed) a table returning an Extract instance
           :param table_name: name of table to extract
           :param expire_seconds: will trigger re-introspection if expired
           :param partitioning_threshold: do not partition tables with less than this many rows (default: ``1,000,000``
        """
        if extract.introspect_date is not None:
            duration = extract.introspect_date - datetime.now()
            if duration.total_seconds() > self._expire_seconds:
                self._introspect(extract)
        else:
            self._introspect(extract)
        return extract

    def _extract(self, extract: Extract, uri: str) -> str:
        session = self._spark_session
        # spark.sparkContext.setJobGroup(table.name, "full extract")
        if extract.predicates is not None and len(extract.predicates) > 0:
            session.sparkContext.setJobDescription(
                f'{extract.name} ({len(extract.predicates)} predicates)')
            df = session.read.jdbc(
                self.config.jdbc.url,
                table=extract.name,
                predicates=extract.predicates,
                properties=self.config.jdbc.properties
            )
        elif extract.partition_column is not None and extract.min is not None and extract.max is not None:
            session.sparkContext.setJobDescription(
                f'{extract.name} (partitioned on [{extract.partition_column}] from {extract.min} to {extract.max})')
            df = session.read.jdbc(
                url=self.config.jdbc.url,
                table=extract.name,
                column=extract.partition_column,
                lowerBound=str(extract.min),
                upperBound=str(extract.max),
                numPartitions=extract.partitions,
                properties=self.config.jdbc.properties
            )
        else:
            # Simple table dump
            session.sparkContext.setJobDescription(
                f'{extract.name}')
            df = session.read.jdbc(
                url=self.config.jdbc.url,
                table=extract.name,
                properties=self.config.jdbc.properties
            )

        # Always normalize table name
        n_table_name = normalize_str(extract.name)
        if self.config.normalize_schema:
            # Normalize column names?
            df = self.normalize_df(df)

        # Attempted different methods of attaching listeners to get row count, none were reliable

        session.sparkContext.setLocalProperty("callSite.short", n_table_name)

        logger.debug(f"Extracting {extract.name} as {n_table_name}")
        df.write.save(f"{uri}/{n_table_name}", format=self.config.spark.format, mode="overwrite",
                      timestampFormat=self.config.spark.timestamp_format, compression=self.config.spark.compression)

        final_uri = f"{uri}/{n_table_name}/part-*.{self.config.spark.format.lower()}" + \
            (".gz" if self.config.spark.compression == "gzip" else "")

        return final_uri

    def extract(self, extract: Extract) -> Extract:
        # Extract the table with Spark
        extract.extract_uri = None
        if self.config.target_uri is not None:
            logger.info(
                f"Extracting {extract.name} ({extract.partitions} partitions)")
            extract_uri = self._extract(extract, self.config.target_uri)
            extract.extract_uri = extract_uri
            extract.extract_date = datetime.now()

            # Suggest a recommended partition size based on the actual extract size (for next run)
            if extract.partitions > 1 and "gs://" in extract_uri:  # only resizes based on GCS targets, for now
                extract.gcs_bytes = self.retryer(get_size_bytes, extract_uri)
                recommendation = ceil(
                    extract.gcs_bytes / self.config.target_partition_size_bytes)
                if recommendation != extract.partitions:
                    logger.info(
                        f"Adjusted partitions on {extract.name} from {extract.partitions} to {recommendation} for next run")
                    extract.partitions = recommendation
                    extract.introspect_date = None  # triggers new introspection next run

            # Write table schema
            json_schema = json.dumps(extract.bq_schema, indent=4)
            if "gs://" not in self.config.target_uri:
                with open(f"{self.config.target_uri}/{normalize_str(extract.name)}/schema.json", "wt") as f:
                    f.write(json_schema)
            else:
                self.retryer(upload_from_string, json_schema,
                             f"{self.config.target_uri}/{normalize_str(extract.name)}/schema.json")

        return extract

    def load(self, extract: Extract) -> Extract:

        # Load into BigQuery
        if self.config.target_dataset is not None:
            # Load from GCS into BQ
            normalized_table_name = normalize_str(extract.name)
            bq_rows: int = 0
            bq_bytes: int = 0
            if self.config.target_dataset is not None:
                logger.info(
                    f"Loading {extract.name} into BigQuery as {normalized_table_name} from {extract.extract_uri}")
                bq_rows, bq_bytes = self.retryer(bigquery_load, extract.extract_uri, f"{self.config.target_dataset}.{normalized_table_name}",
                                                 self.config.spark.format, extract.bq_schema, "Loaded by Dumpty")
            extract.rows_loaded = bq_rows
            extract.bq_bytes = bq_bytes

        return extract

    def reconcile(self, table_names: List[str]):
        """Checks if a list of table names exist in TinyDB and SQL database
            :param table_names: List of table names to validate against database
            :raises: :class:`.ValidationException` when a table is not found
        """
        logger.info(f"Enumerating tables in schema {self.config.schema}")
        sql_tables = self._inspector.get_table_names(schema=self.config.schema)
        not_found = [t for t in table_names if t not in sql_tables]
        if len(not_found) > 0:
            raise ValidationException(
                f"Could not find these tables in {self.config.schema}: {','.join(not_found)}")
