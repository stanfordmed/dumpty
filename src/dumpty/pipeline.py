import traceback
from dataclasses import dataclass
from datetime import datetime
from math import ceil
from queue import Empty, Queue
from random import uniform
from threading import Thread
from time import sleep
from typing import Callable, List

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from sqlalchemy import Column, MetaData, Table, func, inspect, literal_column
from sqlalchemy.engine import Engine, Inspector
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import Session
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import *
from sqlalchemy.sql.expression import cast
from tenacity import Retrying

from dumpty import logger
from dumpty.config import Config
from dumpty.exceptions import ExtractException, ValidationException
from dumpty.extract import Extract
from dumpty.gcp import GCP
from dumpty.util import normalize_str, count_big


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
        self._shutdown = False
        super().__init__()
        self.daemon = True
        self.start()

    def run(self):
        while not self._shutdown:
            try:
                extract: Extract = self.step.in_queue.get(timeout=1)
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
            except Empty:
                pass

    def shutdown(self):
        self._shutdown = True


class QueueWorkerPool():
    def __init__(self, step: Step, size: int):
        self.step = step
        self.workers = []
        for _ in range(size):
            self.workers.append(QueueWorker(step))

    def busy_count(self):
        return sum(worker.busy for worker in self.workers)

    def shutdown(self):
        for worker in self.workers:
            worker.shutdown()


class QueueSubmitter(Thread):
    """Submits items to a queue in a background thread, waiting 0.0-0.25s between each to avoid hammering
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
            sleep(uniform(0.0, 0.25))


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
        self.gcp = GCP()
        self._metadata = MetaData(bind=engine, schema=config.schema)
        self._inspector: Inspector = inspect(engine)

        self.introspect_queue = Queue(config.introspect_workers)
        self.extract_queue = Queue(config.extract_workers)
        self.load_queue = Queue(config.load_workers)
        self.done_queue = Queue()
        self.error_queue = Queue()

        # Setup ELT steps and queues
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
            elif isinstance(col.type, (String, UNIQUEIDENTIFIER)):
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
        self.introspect_workers.shutdown()
        self.extract_workers.shutdown()
        self.load_workers.shutdown()
        self._spark_session.stop()

    def status(self) -> str:
        return f"Introspecting:{self.introspect_workers.busy_count()} | Extracting:{self.extract_workers.busy_count()} | Loading:{self.load_workers.busy_count()}"

    def submit(self, extracts: List[Extract]):
        """Starts a thread submitting extracts to the introspect_queue and returns immediately

        Args:
            extracts (List[Extract]): Extracts to start introspecting
        """
        QueueSubmitter(extracts, self.introspect_queue)

    def _julienne(self, table: Table, column: Column, width: int):
        logger.debug(
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

            # Spark predicates can't use parameterized queries, so we can't use native Python types
            # and let python/jdbc handle type conversion. So we cast to a String (VARCHAR) here
            # for anything non-numeric (eg. datetime.datetime) and hope that tranlates
            # properly in the other direction, in the spark predicate where clause..
            if isinstance(column.type, sqltypes.Numeric):
                query = session\
                    .query(func.distinct(subquery.c.id), subquery.c.row_num)\
                    .filter(modulo_filter == 0)\
                    .order_by(subquery.c.row_num)
            else:
                query = session\
                    .query(func.distinct(cast(subquery.c.id, String)), subquery.c.row_num)\
                    .filter(modulo_filter == 0)\
                    .order_by(subquery.c.row_num)

            result = [r[0] for r in query.all()]
            return result

    def introspect(self, extract: Extract) -> Extract:
        """Introspects a SQL table: row counts, min, max, and partitions

        Args:
            extract (Extract): Extract instance to introspect

        Returns:
            Extract: Updated extract object (same object as input)
        """
        # Introspect table from SQL database
        table = Table(extract.name, self._metadata, autoload=True)

        # Create BQ schema definition
        extract.bq_schema = self.bq_schema(table)

        if extract.introspect_date is not None:
            # This table was introspected, is it time to refresh?
            if self.config.introspection_expire_s > 0:
                if (datetime.now() - extract.introspect_date).total_seconds() > self.config.introspection_expire_s:
                    # Introspection has expired
                    logger.info(
                        f"Introspection for {extract.name} has expired")
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

        logger.debug(
            f"{'Deep' if full_introspect else 'Fast'} introspecting {extract.name}")

        if self.engine.dialect.name == "mssql":
            # MSSQL COUNT(*) can overflow if > INT_MAX
            count_fn = count_big
        else:
            count_fn = func.count

        if full_introspect:
            if len(table.primary_key.columns) > 0:
                # First PK guaranteed(?) to be indexed, so we use that
                pk = table.primary_key.columns[0]
                if extract.partition_column != pk.name:
                    # Partition column has changed since last introspection, reset the partition count
                    extract.partitions = None
            else:
                pk = None
        else:
            pk = table.primary_key.columns[extract.partition_column] if extract.partition_column is not None else None

        with Session(self.engine) as session:
            is_numeric = pk is not None and isinstance(
                pk.type, sqltypes.Numeric)
            if is_numeric and full_introspect:
                logger.debug(
                    f"Getting min({pk.name}), max({pk.name}), and count(*) of {extract.name}")
                qry = session.query(func.max(pk).label("max"),
                                    func.min(pk).label("min"),
                                    count_fn(
                                        literal_column("*")).label("count")
                                    ).select_from(table)
                res = qry.one()
                extract.max = res.max
                extract.min = res.min
                extract.rows = res.count
            else:
                logger.debug(f"Getting count(*) of {extract.name}")
                qry = session.query(
                    count_fn(literal_column("*")).label("count")
                ).select_from(table)
                extract.max = None
                extract.min = None
                extract.rows = qry.scalar()

        if not full_introspect:
            # Stop here if this table was already introspected recently
            extract.refresh_date = datetime.now()
            return extract

        # Continue with full introspection, reset partitioning
        extract.partition_column = None
        extract.predicates = None

        # Only partition tables with a PK and would generate at least two partitions when rounded up
        if pk is not None and extract.rows > 0:
            partitions = round(
                extract.rows / self.config.default_rows_per_partition) if extract.partitions is None else extract.partitions
            if partitions > 1:
                extract.partitions = partitions
                extract.partition_column = pk.name
                slice_width = ceil(extract.rows / extract.partitions)

                if is_numeric and ((extract.rows == extract.max) or (extract.rows == extract.max - 1) or (abs(extract.rows - (extract.max - extract.min)) <= 1)):
                    # Numeric, sequential PK with no gaps uses default Spark column partitioning
                    logger.info(
                        f"{extract.name} using Spark partitioning on {pk.name} ({partitions} partitions)")
                else:
                    # Non-numeric, or PK is not sequential and likely heavily skewed, julienne the table instead
                    slices = self._julienne(table, pk, slice_width)
                    if len(slices) / partitions < 0.10:
                        logger.warning(
                            f"Failed to Julienne {extract.name} on {pk.name}, not enough distinct PK values. Using single-threaded extract.")
                        extract.predicates = None
                        extract.partition_column = None
                        extract.partitions = None
                    else:
                        quote_char = "" if is_numeric else "'"
                        predicates = []
                        for i in range(len(slices)+1):
                            if i == 0:
                                predicates.append(
                                    f"{pk.name} <= {quote_char}{slices[i]}{quote_char} OR {pk.name} IS NULL ")
                            elif i == len(slices):
                                predicates.append(
                                    f"{pk.name} > {quote_char}{slices[i-1]}{quote_char}")
                            else:
                                predicates.append(
                                    f"{pk.name} > {quote_char}{slices[i-1]}{quote_char} AND {pk.name} <= {quote_char}{slices[i]}{quote_char}")
                        extract.predicates = predicates
                        logger.info(
                            f"{extract.name} using predicate partitioning on {pk.name} ({partitions} predicates)")

        now = datetime.now()
        extract.introspect_date = now
        extract.refresh_date = now

        return extract

    def _extract(self, extract: Extract, uri: str) -> str:

        session = self._spark_session
        if session.sparkContext._jsc is None:
            raise ExtractException(
                extract, f"Spark context lost trying to extract {extract.name}, is Spark shutting down?")

        session = self._spark_session
        if session.sparkContext._jsc is None:
            raise ExtractException(
                extract, f"Spark context lost trying to extract {extract.name}, is Spark shutting down?")

        # Always normalize table name
        n_table_name = normalize_str(extract.name)

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
                f"Extracting {extract.name} as {n_table_name} ({len(extract.predicates)} predicates)")
        elif extract.partition_column is not None and extract.min is not None and extract.max is not None and extract.partitions > 1:
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
        """Submits an Extract object to Spark for dumping

        Args:
            extract (Extract): Extract instance to dump

        Returns:
            Extract: Extracted instance (same as input)
        """
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
            # only resizes based on GCS targets, for now
            if extract.partitions is not None and extract.partitions > 0 and "gs://" in extract_uri:
                extract.gcs_bytes = self.retryer(
                    self.gcp.get_size_bytes, extract_uri)
                if extract.gcs_bytes < self.config.target_partition_size_bytes:
                    # Table does not need partitioning
                    logger.info(
                        f"{extract.name} < {self.config.target_partition_size_bytes} bytes, will no longer partition")
                    extract.partition_column = None
                    extract.predicates = None
                    extract.partitions = None
                else:
                    recommendation = round(
                        extract.gcs_bytes / self.config.target_partition_size_bytes)
                    if recommendation > 1 and recommendation != extract.partitions:
                        logger.info(
                            f"Adjusted partitions on {extract.name} from {extract.partitions} to {recommendation} for next run")
                        extract.partitions = recommendation
                        extract.introspect_date = None  # triggers new introspection next run

        return extract

    def load(self, extract: Extract) -> Extract:
        """Loads an Extract into BigQuery

        Args:
            extract (Extract): Extract instance to load into BigQuery

        Returns:
            Extract: Extract instance that was loaded (same object as input)
        """

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
                    bq_rows, bq_bytes = self.retryer(self.gcp.bigquery_load, extract.extract_uri, f"{self.config.target_dataset}.{normalized_table_name}",
                                                     self.config.spark.format, extract.bq_schema, "Loaded by Dumpty")
                extract.rows_loaded = bq_rows
                extract.bq_bytes = bq_bytes
            else:
                # Create empty table directly
                self.gcp.bigquery_create_table(
                    f"{self.config.target_dataset}.{normalized_table_name}", extract.bq_schema)
                extract.rows_loaded = 0
                extract.bq_bytes = 0

        return extract

    def reconcile(self, table_names: List[str]):
        """Checks if a list of table names exist in TinyDB and SQL database
            :param table_names: List of table names to validate against database
            :raises: :class:`.ValidationException` when a table is not found. Use this
            to fail early if you are not sure if the tables are actually in the SQL database.
        """
        logger.info(f"Enumerating tables in schema {self.config.schema}")
        sql_tables = self._inspector.get_table_names(schema=self.config.schema)
        not_found = [t for t in table_names if t not in sql_tables]
        if len(not_found) > 0:
            raise ValidationException(
                f"Could not find these tables in {self.config.schema}: {','.join(not_found)}")
