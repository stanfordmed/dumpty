import argparse
import json
import os
import logging
from typing import List
import psutil
import sys
from math import ceil, floor
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import create_engine, engine_from_config
from sqlalchemy.sql import sqltypes
from alive_progress import alive_bar
from tenacity import Retrying, wait_random_exponential, stop_after_delay, stop_after_attempt
from tinydb import JSONStorage, TinyDB, Query
from tinydb.table import Document
from dumpty import gcp
from dumpty.config import Config
from dumpty.spark import LocalSpark
from dumpty.gcp import upload_from_string, bigquery_load, bigquery_create_dataset
from dumpty.sql import Sql, factory
from dumpty.util import normalize_str, filter_shuffle
from tinydb_serialization.serializers import DateTimeSerializer
from tinydb_serialization import SerializationMiddleware
from tinydb.middlewares import CachingMiddleware
from tinydb import where
from dumpty import logger


def config_from_args(argv) -> Config:
    parser = argparse.ArgumentParser(
        description="Unnamed database export utility")

    parser.add_argument('--spark-loglevel', default='WARN', dest='spark_loglevel',
                        help='Set Spark logging level: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN(default)')

    parser.add_argument('--project', type=str,
                        help='Project ID for API calls (default: infer from environment)')

    parser.add_argument('--credentials', type=str,
                        help='JSON credentials file (default: infer from environment)')

    parser.add_argument('--drop', action='store_true', default=None,
                        help='Drop the destination dataset before creating it')

    parser.add_argument('--verbose', action='store_true', default=None,
                        help='Enable verbose (debug) logging')

    parser.add_argument('--no-progress', action='store_true', dest='progress',
                        help='Do not show progress bar')

    parser.add_argument('--config', type=str,
                        help='Jinja2 templated YAML config file', default='config.yaml')

    parser.add_argument('--logfile', type=str,
                        help='JSON log filename (default: extract.json)')

    parser.add_argument('--parse', action='store_true', dest='parse',
                        help='Print parsed config file and exit')

    parser.add_argument('uri', type=str, nargs='?',
                        help='Local path or gs:// URI to store extracted data')

    parser.add_argument('dataset', type=str, nargs='?',
                        help='project.dataset to load extract (requires gs:// uri)')

    args = parser.parse_args(argv)

    # Set default log level to INFO
    logger.setLevel(logging.INFO)

    # Parses YAML as a bare Jina2 template (no round-trip parsing)
    template: Template = Environment(loader=FileSystemLoader('.')).from_string(
        Path(args.config).read_text())

    template.environment.filters['shuffle'] = filter_shuffle
    parsed = template.render()
    if args.parse:
        print(parsed)
        sys.exit()
    config = Config.from_yaml(parsed)

    # Command line args override config file
    if args.uri is not None:
        config.target_uri = args.uri
    if args.dataset is not None:
        config.target_dataset = args.dataset
    if args.drop is not None:
        config.drop_dataset = args.drop
    if args.spark_loglevel is not None:
        config.spark.log_level = args.spark_loglevel
    if args.logfile is not None:
        config.log_file = args.logfile
    if args.progress is not None:
        config.progress_bar = not args.progress
    if args.project is not None:
        config.project = args.project
    if args.credentials is not None:
        config.credentials = args.credentials
    if args.verbose is not None:
        logger.setLevel(logging.DEBUG)

    if config.target_dataset is not None and "." not in config.target_dataset:
        parser.error("Dataset must be in format project.dataset")

    if config.target_uri is not None and "gs://" not in config.target_uri:
        parser.error(
            f"Loading a dataset requires gs:// URI (uri is {config.target_uri}")

    if config.project is not None:
        os.environ['GOOGLE_CLOUD_PROJECT'] = config.project
    if config.credentials is not None:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.credentials

    return config


def introspect(table: Document, sql: Sql):
    table_name = table['name']
    start_time = datetime.now()
    logger.debug(f"Introspecting {sql.schema}.{table_name}")

    # Get primary key info, min, max, row count
    rows = 0
    pk = sql.get_pk(table_name)
    if pk is not None:
        table['pk'] = pk.name
        (min, max, rows) = sql.get_min_max_count(table_name, pk.name)
        table['min'] = min
        table['max'] = max
        table['rows'] = rows
        is_numeric = isinstance(pk.type, sqltypes.Numeric)

    if pk is not None and rows > 1e6:  # don't bother splitting any tables smaller than 1m rows
        partitions = table.get('partitions')

        if not partitions:
            # Default to ~1M rows per partition, but at least two partitions
            partitions = ceil(rows / 1e6)

        if table.get('partitions_rec'):
            partitions = table.get('partitions_rec')

        if partitions > 1:
            slice_width = floor(rows / partitions)
            if is_numeric:
                # Numeric, sequential PK with no gaps (starting with an offset is ok)
                # will use default Spark column partitioning
                if (rows == max) or (rows == max - 1) or (abs(rows - (max - min)) <= 1):
                    table['partition_column'] = pk.name
                else:
                    # Numeric, PK is not sequential and likely heavily skewed
                    intervals = sql.julienne(table_name, pk.name, slice_width)
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
                    table['predicates'] = slices
            else:
                # Row-number based splitting on a non-numeric field
                intervals = sql.julienne(table_name, pk.name, slice_width)
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
                table['predicates'] = slices
        else:
            # Recommendation from last run was to not split this table, remove old entries
            table['partition_column'] = None
            table['predicates'] = None
    else:
        partitions = 1

    table['partitions'] = partitions

    complete_time = datetime.now()
    duration = complete_time - start_time
    table['introspected'] = complete_time
    table['introspection_s'] = duration.total_seconds()


def extract_and_load(table: Document, config: Config, spark: LocalSpark, sql: Sql, retryer: Retrying) -> Document:
    table_name = table['name']

    introspected = table.get('introspected')
    expired = introspected and config.introspection_expire_s > 0 and (
        (datetime.now() - introspected).total_seconds() >= config.introspection_expire_s)
    if not introspected or expired:
        retryer(introspect, table, sql)
    else:
        # Check if there is a recommended partition size from a previous run, if so re-calculate partitioning
        rec = table.get('partitions_rec')
        if rec and rec != table['partitions']:
            retryer(introspect, table, sql)
        else:
            # Just get a fresh row count
            table['rows'] = sql.get_row_count(table_name)

    # Extract the table with Spark
    if config.target_uri is not None:
        extract_uri = retryer(spark.full_extract, table, config.target_uri)
        table['extract_uri'] = extract_uri

        # Suggest a recommended partition size based on the actual extract size (for next run)
        if "gs://" in extract_uri:  # only GCS for now
            storage_bytes = gcp.get_size_bytes(extract_uri)
            rec = ceil(storage_bytes / config.target_partition_size_bytes)
            if rec != table.get('partitions', 1):
                logger.debug(
                    f"Recommending {rec} partitions for {table_name} for next run")
            table['partitions_rec'] = rec
            table['storage_bytes'] = storage_bytes

        # Get JSON-formatted table schema
        schema = sql.get_schema(table_name)
        json_schema = json.dumps(schema, indent=4)
        if "gs://" not in config.target_uri:
            with open(f"{config.target_uri}/{normalize_str(table_name)}/schema.json", "wt") as f:
                f.write(json_schema)
        else:
            retryer(upload_from_string, json_schema,
                    f"{config.target_uri}/{normalize_str(table_name)}/schema.json")

        # Load into BigQuery
        if config.target_dataset is not None:
            # Load from GCS into BQ
            bq_rows: int = 0
            bq_bytes: int = 0
            if config.target_dataset is not None:
                bq_rows, bq_bytes = retryer(bigquery_load, extract_uri, f"{config.target_dataset}.{table_name}",
                                            config.spark.format, schema, "Loaded by dumpty")
            table['rows_loaded'] = bq_rows
            table['bytes_loaded'] = bq_bytes

    return table


def main(args=None):

    config = config_from_args(args)

    # Initialize SqlAlchemy
    engine = create_engine(config.sqlalchemy.url, pool_size=config.sqlalchemy.pool_size, connect_args=config.sqlalchemy.connect_args,
                           max_overflow=config.sqlalchemy.max_overflow, pool_pre_ping=True)
    sql = factory(config.schema, engine)

    # Default retry for network operations: 2^x * 1 second between each retry, starting with 5s, up to 60s, die after 5 minutes of retries
    # reraise=True places the exception at the END of the stack-trace dump
    retryer = Retrying(wait=wait_random_exponential(multiplier=1, min=5, max=60), stop=(
        stop_after_delay(300) | stop_after_attempt(0 if not config.retry else 999)), reraise=True)

    # Create destination dataset
    if config.target_dataset is not None:
        retryer(bigquery_create_dataset,
                dataset_ref=config.target_dataset, drop=config.drop_dataset)

    # Create spark logdir if needed
    spark_log_dir = config.spark.properties.get('spark.eventLog.dir')
    if spark_log_dir is not None:
        if not os.path.exists(spark_log_dir):
            os.makedirs(spark_log_dir)

    # summary = ExtractSummary(tables=[], warnings=[])
    summary = {
        "start_date": datetime.now(),
        "tables": [],
        "warnings": []
    }

    # TinyDB doesn't support DateTime out of the box so we add it here
    serialization = SerializationMiddleware(JSONStorage)
    serialization.register_serializer(DateTimeSerializer(), 'TinyDate')

    extract_jobs = []
    with TinyDB('tables.json', sort_keys=True, indent=4,
                storage=CachingMiddleware(serialization)) as db:
        db.default_table_name = config.schema

        # Check if new tables have been added to config.tables since last run
        tiny_db = db.all()
        new_tables = [t for t in config.tables if t not in [
            d['name'] for d in tiny_db]]
        if len(new_tables) > 0:
            # Make sure these tables _actually_ exist in the SQL database before going any further
            sql_tables = sql.get_tables()
            not_found = [t for t in new_tables if t not in sql_tables]
            if len(not_found) > 0:
                logger.error(
                    f"Tables listed in config schema were not found in SQL schema {config.schema}: {','.join(not_found)}")
                exit(1)
            db.insert_multiple([{"name": t} for t in new_tables])

        with LocalSpark(config, config.target_uri) as spark:
            # Throttles the number of jobs submitted to Spark (and # concurrent pysql DB connections)
            with ThreadPoolExecutor(max_workers=config.job_threads) as executor:
                for table in config.tables:
                    extract_jobs.append(executor.submit(extract_and_load, db.search(Query().name == table)[0], config,
                                                        spark, sql, retryer))
                with alive_bar(len(extract_jobs), dual_line=True, stats=False, disable=not config.progress_bar) as bar:
                    for future in as_completed(extract_jobs):
                        try:
                            extracted_table: Document = future.result()
                            db.update(extracted_table, Query().name ==
                                      extracted_table['name'])
                            logger.debug(f"{extracted_table['name']} complete")
                        except Exception as ex:
                            logger.error(ex)
                            executor.shutdown(
                                wait=False, cancel_futures=True)
                            raise ex
                        summary['tables'].append(extracted_table)
                        if config.target_dataset is not None:
                            consistent = extracted_table['rows'] == extracted_table['rows_loaded']
                            if not consistent:
                                warning = f"{extracted_table['name']}: row count mismatch (expected: {extracted_table['rows']}, loaded: {extracted_table['rows_loaded']}+"
                                logger.warning(warning)
                                summary.warnings.append(warning)
                        bar.text = f"| {datetime.now().strftime('%H:%M:%S')} | System CPU: {psutil.cpu_percent()}% | Memory: {psutil.virtual_memory()[2]}%"
                        bar()
                logger.debug("Extraction complete, shutting down")

    # Summarize
    summary['end_date'] = datetime.now()
    summary['elapsed_s'] = round(
        (summary['end_date'] - summary['start_date']).total_seconds())

    if config.target_dataset is not None:
        summary['consistent'] = all(x['rows'] == x['rows_loaded']
                                    for x in summary['tables'])
        summary['total_bytes'] = sum(x['bytes_loaded']
                                     for x in summary['tables'])
        summary['mb_per_second'] = round(
            (summary['total_bytes'] / summary['elapsed_s']) / pow(10, 6), 2)

    with open(config.log_file, "w") as outfile:
        outfile.write(json.dumps(summary, indent=4, default=str))

    logging.info(
        f"Extract summary saved to {config.log_file}")

    if not len(summary['warnings']) == 0:
        logging.warning(
            f"{len(summary['tables'])} tables loaded, with warnings")


if __name__ == '__main__':
    main()