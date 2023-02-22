import argparse
import json
import logging
import os
import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from math import ceil
from pathlib import Path
from typing import List

import psutil
from alive_progress import alive_bar
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import create_engine
from tenacity import (Retrying, stop_after_attempt, stop_after_delay,
                      wait_random_exponential)

from dumpty import logger
from dumpty.config import Config
from dumpty.gcp import (bigquery_create_dataset, bigquery_load,
                        upload_from_string, get_size_bytes)
from dumpty.introspector import Extract, Introspector
from dumpty.spark import LocalSpark
from dumpty.util import filter_shuffle, normalize_str


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


def extract_and_load(table_name: str, config: Config, spark: LocalSpark, introspector: Introspector, retryer: Retrying):
    # Introspect if needed
    extract = retryer(introspector.introspect, table_name,
                      config.partitioning_threshold)

    # Extract the table with Spark
    extract.extract_uri = None
    if config.target_uri is not None:
        extract_uri = retryer(spark.full_extract, extract, config.target_uri)
        extract.extract_uri = extract_uri
        extract.extract_date = datetime.now()

        # Suggest a recommended partition size based on the actual extract size (for next run)
        if extract.partitions > 0 and "gs://" in extract_uri:  # only resizes based on GCS targets, for now
            extract.gcs_bytes = get_size_bytes(extract_uri)
            recommendation = ceil(
                extract.gcs_bytes / config.target_partition_size_bytes)
            if recommendation != extract.partitions:
                logger.info(
                    f"Adjusted partitions on {extract.name} from {extract.partitions} to {recommendation} for next run")
                extract.partitions = recommendation
                extract.introspect_date = None  # triggers new introspection next run

        # Get JSON-formatted table schema
        json_schema = json.dumps(extract.bq_schema, indent=4)
        if "gs://" not in config.target_uri:
            with open(f"{config.target_uri}/{normalize_str(extract.name)}/schema.json", "wt") as f:
                f.write(json_schema)
        else:
            retryer(upload_from_string, json_schema,
                    f"{config.target_uri}/{normalize_str(extract.name)}/schema.json")

        # Load into BigQuery
        if config.target_dataset is not None:
            # Load from GCS into BQ
            normalized_table_name = normalize_str(extract.name)
            bq_rows: int = 0
            bq_bytes: int = 0
            if config.target_dataset is not None:
                logger.info(
                    f"Loading {extract.name} into BigQuery as {normalized_table_name} from {extract.extract_uri}")
                bq_rows, bq_bytes = retryer(bigquery_load, extract_uri, f"{config.target_dataset}.{normalized_table_name}",
                                            config.spark.format, extract.bq_schema, "Loaded by Dumpty")
            extract.rows_loaded = bq_rows
            extract.bq_bytes = bq_bytes

    return extract


def main(args=None):

    config: Config = config_from_args(args)

    # Initialize SqlAlchemy
    engine = create_engine(config.sqlalchemy.url, pool_size=config.sqlalchemy.pool_size, connect_args=config.sqlalchemy.connect_args,
                           max_overflow=config.sqlalchemy.max_overflow, pool_pre_ping=True, echo=False)

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

    summary = {
        "start_date": datetime.now(),
        "tables": [],
        "warnings": []
    }

    extract_jobs: List[Future] = []
    completed: List[Extract] = []
    with Introspector(config.tinydb_database_file, engine, config.schema) as introspector:

        if config.reconcile:
            # Check if tables being requested actually exist in SQL database before doing anything else
            # This can be very slow for databases with thousands of tables so it is off by default
            introspector.reconcile(config.tables)

        with LocalSpark(config, config.target_uri) as spark:

            # Throttles the number of jobs submitted to Spark (and # concurrent pysql DB connections)
            with ThreadPoolExecutor(max_workers=config.job_threads) as executor:

                for table in config.tables:
                    extract_jobs.append(executor.submit(
                        extract_and_load, table, config, spark, introspector, retryer))

                with alive_bar(len(extract_jobs), dual_line=True, stats=False, disable=not config.progress_bar) as bar:
                    for future in as_completed(extract_jobs):
                        try:
                            extracted_table: Extract = future.result()
                            introspector.save(extracted_table)
                            completed.append(extracted_table)
                            logger.debug(f"{extracted_table.name} complete")
                        except Exception as ex:
                            logger.error(ex)
                            executor.shutdown(
                                wait=False, cancel_futures=True)
                            raise ex
                        summary['tables'].append(extracted_table.name)
                        if config.target_dataset is not None:
                            if not extracted_table.consistent():
                                warning = f"{extracted_table.name}: row count mismatch (expected: {extracted_table.rows}, loaded: {extracted_table.rows_loaded}+"
                                logger.warning(warning)
                                summary['warnings'].append(warning)
                        bar.text = f"| {datetime.now().strftime('%H:%M:%S')} | System CPU: {psutil.cpu_percent()}% | Memory: {psutil.virtual_memory()[2]}%"
                        bar()

                logger.info("Extraction complete, shutting down")

    # Summarize
    summary['end_date'] = datetime.now()
    summary['elapsed_s'] = round(
        (summary['end_date'] - summary['start_date']).total_seconds())

    if config.target_dataset is not None:
        summary['consistent'] = all(x.consistent() for x in completed)
        summary['bq_bytes'] = sum(x.bq_bytes for x in completed)

    if config.target_uri is not None:
        summary['gcs_bytes'] = sum(x.gcs_bytes for x in completed)

    with open(config.log_file, "w") as outfile:
        outfile.write(json.dumps(summary, indent=4, default=str))

    if not len(summary['warnings']) == 0:
        logger.warning(
            f"{len(summary['tables'])} tables loaded, with warnings")


if __name__ == '__main__':
    main()
