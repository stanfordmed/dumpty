import json
import traceback
from dataclasses import dataclass
from datetime import datetime
from math import ceil, floor
from queue import Queue
from random import uniform
from threading import Thread
from time import sleep
from typing import Callable, List

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from sqlalchemy import Column, MetaData, Table, func, inspect
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.orm import Session
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import *
from tenacity import Retrying

from dumpty import logger
from dumpty.config import Config
from dumpty.exceptions import ExtractException, ValidationException
from dumpty.extract import Extract
from dumpty.gcp import bigquery_create_table, bigquery_load, get_size_bytes
from dumpty.util import normalize_str


@dataclass
class Step:
    """Initialize a :class:`.Step` instance. 
       :param func: Function to apply to item received from :param in_queue:
       :param in_queue: Queue to receive work for :param func:
       :param out_queue: Queue to send result of applying :param func: :param in_queue:
       :param error_queue: Queue to send exceptions from applying :param func:
    """
    func: Callable
    in_queue: Queue
    out_queue: Queue
    error_queue: Queue


class QueueWorker(Thread):

    def __init__(self, step: Step):
        self.step = step
        self.busy = False
        super().__init__()
        self.daemon = True
        self.start()

    def run(self):
        while True:
            extract: Extract = self.step.in_queue.get()
            try:
                self.busy = True
                self.step.out_queue.put(self.step.func(extract))
            except Exception as ex:
                try:
                    raise ExtractException(extract) from ex
                except ExtractException as ex:
                    logger.error(ex)
                    traceback.print_exc()
                    self.step.error_queue.put(ex)
            finally:
                self.busy = False
                self.step.in_queue.task_done()


class QueueWorkerPool():
    def __init__(self, step: Step, size: int):
        self.step = step
        self.workers = []
        for _ in range(size):
            self.workers.append(QueueWorker(step))

    def busy_count(self):
        return sum(worker.busy for worker in self.workers)


class QueueSubmitter(Thread):
    """Submits items to a queue, waiting up to one second between each to avoid bursting
    """

    def __init__(self, items, queue: Queue):
        self.items = items
        self.queue = queue
        super().__init__()
        self.daemon = True
        self.start()

    def run(self):
        for item in self.items:
            self.queue.put(item)
            sleep(uniform(0.0, 1.0))


class Pipeline:
    """Main class for the various stages of the ELT process
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

        self.introspect_queue = Queue(config.introspect_workers)
        self.extract_queue = Queue(config.extract_workers)
        self.load_queue = Queue(config.load_workers)
        self.done_queue = Queue()
        self.error_queue = Queue()

        # Setup queues
        introspect_step = Step(
            self.introspect, self.introspect_queue, self.extract_queue, self.error_queue)

        extract_step = Step(
            self.extract, self.extract_queue, self.load_queue, self.error_queue)

        load_step = Step(self.load, self.load_queue,
                         self.done_queue, self.error_queue)

        self.introspect_workers = QueueWorkerPool(
            introspect_step, self.introspect_queue.maxsize)

        self.extract_workers = QueueWorkerPool(
            extract_step, self.extract_queue.maxsize)

        self.load_workers = QueueWorkerPool(load_step, self.load_queue.maxsize)

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

    @staticmethod
    def normalize_df(df: DataFrame) -> DataFrame:
        return df.select([col(x).alias(normalize_str(x)) for x in df.columns])

    @staticmethod
    def bq_schema(table: Table) -> list[dict]:
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

    def shutdown(self):
        self._spark_session.stop()

    def status(self) -> str:
        return f"Introspecting: {self.introspect_workers.busy_count()} | Extracting {self.extract_workers.busy_count()} | Loading {self.load_workers.busy_count()}"

    def submit(self, extracts: List[Extract]):
        QueueSubmitter(extracts, self.introspect_queue)

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

    def introspect(self, extract: Extract):

        # Introspect table from SQL database
        table = Table(extract.name, self._metadata, autoload=True)

        if extract.introspect_date is not None:
            # This table was introspected, is it time to refresh?
            if self.config.introspection_expire_s > 0:
                if (datetime.now() - extract.introspect_date).total_seconds() > self.config.introspection_expire_s:
                    # Introspection has expired
                    full_introspect = True
                else:
                    # Introspection has not expired
                    full_introspect = False
            else:
                # Introspections _never_ expire
                full_introspect = False
        else:
            # Never been introspected, or partitioning was modified from prior run
            full_introspect = True

        logger.info(
            f"{'Deep' if full_introspect else 'Fast'} introspecting {extract.name}")

        # Get row count (and min/max if this is a full introspection)
        pk = table.primary_key.columns[0] if table.primary_key else None
        with Session(self.engine) as session:
            if pk is not None:
                is_numeric = isinstance(pk.type, sqltypes.Numeric)
                if is_numeric and full_introspect:
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
                # If no PK dump the entire table in a single thread
                qry = session.query(func.count().label(
                    "count")).select_from(table)
                extract.max = None
                extract.min = None
                extract.rows = qry.scalar()

        if not full_introspect:
            return extract

        # Only partition tables with a PK and rows exceeding partition_row_min
        if extract.rows >= self.config.partitioning_threshold and pk is not None:

            # Default to ~1M rows per partition
            if extract.partitions is None:
                extract.partitions = round(extract.rows / 1e6)

            if extract.partitions > 0:
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
                                slices.append(
                                    f"{pk.name} > {intervals[i-1]}")
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
                            slices.append(
                                f"{pk.name} > '{intervals[i-1]}'")
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
        extract.bq_schema = self.bq_schema(table)

        extract.introspect_date = datetime.now()

        return extract

    def _extract(self, extract: Extract, uri: str) -> str:
        # Always normalize table name
        n_table_name = normalize_str(extract.name)

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
            logger.info(
                f"Extracting {extract.name} as {n_table_name} ({len(extract.predicates)} slices)")

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
            logger.info(
                f"Extracting {extract.name} as {n_table_name} (partitioning on {extract.partition_column})")
        else:
            # Simple table dump
            session.sparkContext.setJobDescription(
                f'{extract.name}')
            df = session.read.jdbc(
                url=self.config.jdbc.url,
                table=extract.name,
                properties=self.config.jdbc.properties
            )
            logger.info(
                f"Extracting {extract.name} (single thread)")

        if self.config.normalize_schema:
            # Normalize column names?
            df = self.normalize_df(df)

        session.sparkContext.setLocalProperty("callSite.short", n_table_name)
        df.write.save(f"{uri}/{n_table_name}", format=self.config.spark.format, mode="overwrite",
                      timestampFormat=self.config.spark.timestamp_format, compression=self.config.spark.compression)

        final_uri = f"{uri}/{n_table_name}/part-*.{self.config.spark.format.lower()}" + \
            (".gz" if self.config.spark.compression == "gzip" else "")

        return final_uri

    def extract(self, extract: Extract) -> Extract:
        # Extract the table with Spark
        extract.extract_uri = None

        if extract.rows == 0:
            # Nothing to do here
            return extract

        if self.config.target_uri is not None:
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

        return extract

    def load(self, extract: Extract) -> Extract:

        normalized_table_name = normalize_str(extract.name)

        # Load into BigQuery
        if self.config.target_dataset is not None:
            if extract.rows > 0:
                # Load from GCS into BQ
                bq_rows: int = 0
                bq_bytes: int = 0
                if self.config.target_dataset is not None:
                    logger.info(
                        f"Loading {extract.name} into BigQuery as {normalized_table_name} from {extract.extract_uri}")
                    bq_rows, bq_bytes = self.retryer(bigquery_load, extract.extract_uri, f"{self.config.target_dataset}.{normalized_table_name}",
                                                     self.config.spark.format, extract.bq_schema, "Loaded by Dumpty")
                extract.rows_loaded = bq_rows
                extract.bq_bytes = bq_bytes
            else:
                # Create empty table directly
                bigquery_create_table(
                    f"{self.config.target_dataset}.{normalized_table_name}", extract.bq_schema)
                extract.rows_loaded = 0
                extract.bq_bytes = 0

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
