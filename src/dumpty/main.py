import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from queue import Empty
from typing import List

import psutil
from alive_progress import alive_bar
from tinydb import Query, TinyDB
from dumpty.config import Config
from dumpty.extract import Extract, ExtractDB
from dumpty.pipeline import Pipeline
from dumpty.util import filter_shuffle
from google.api_core.exceptions import BadRequest
from jinja2 import Environment, FileSystemLoader, Template
from sqlalchemy import create_engine
from tenacity import (
    Retrying,
    after_log,
    retry_if_not_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

from dumpty import logger


def config_from_args(argv) -> Config:
    parser = argparse.ArgumentParser(
        description="MS SQL Server Database export utility named DUMPTY"
    )

    parser.add_argument(
        "--spark-loglevel",
        default="WARN",
        dest="spark_loglevel",
        help="Set Spark logging level: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN(default)",
    )

    parser.add_argument(
        "--project",
        type=str,
        help="Project ID for API calls (default: infer from environment)",
    )

    parser.add_argument(
        "--credentials",
        type=str,
        help="JSON credentials file (default: infer from environment)",
    )

    parser.add_argument(
        "--drop",
        action="store_true",
        default=None,
        help="Drop the destination dataset before creating it",
    )

    parser.add_argument(
        "-d",
        "--debug",
        help="Debug logging (DEBUG)",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.WARNING,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        help="Verbose logging (INFO)",
        action="store_const",
        dest="loglevel",
        const=logging.INFO,
    )

    parser.add_argument(
        "--no-progress",
        action="store_true",
        dest="progress",
        help="Do not show progress bar",
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Jinja2 templated YAML config file",
        default="config.yaml",
    )

    parser.add_argument(
        "--logfile", type=str, help="JSON log filename (default: extract.json)"
    )

    parser.add_argument(
        "--fastcount",
        action="store_const",
        const=True,
        help="Rowcount for MSSQL tables with store procedure sp_spaceused",
    )

    parser.add_argument(
        "--parse",
        action="store_true",
        dest="parse",
        help="Print parsed config file and exit",
    )

    parser.add_argument(
        "uri",
        type=str,
        nargs="?",
        help="Local path or gs:// URI to store extracted data",
    )

    parser.add_argument(
        "dataset",
        type=str,
        nargs="?",
        help="project.dataset to load extract (requires gs:// uri)",
    )

    parser.add_argument(
        "--extract",
        type=str,
        help="Flag that indicates whether this is a FULL or INCREMENTAL extract",
    )

    args = parser.parse_args(argv)
    logger.setLevel(args.loglevel)

    # Parses YAML as a bare Jina2 template (no round-trip parsing)
    template: Template = Environment(loader=FileSystemLoader(".")).from_string(
        Path(args.config).read_text()
    )

    template.environment.filters["shuffle"] = filter_shuffle
    parsed = template.render(env=os.environ)
    if args.parse:
        print(parsed)
        sys.exit()
    config = Config.from_yaml(parsed)

    # STORE THE INITIAL VALUE OF LAST SUCCESSFUL RUN IN A TINY DB DATABASE
    db = TinyDB(config.tinydb_date)
    if db.get(Query().name == "last_successful_run") == None:
        db.insert({"name": "last_successful_run", "value": config.last_successful_run})
    # READ THE DATE OF LAST SUCCESSFUL RUN FROM THE TINY DB AND USE IT
    last_successful_run = db.get(Query().name == "last_successful_run").get("value")
    db.close()
    # ADD THE LAST SUCCESSFUL RUN DATE TO THE SQL QUERY
    config.last_successful_run = last_successful_run
    query = config.tables_query.replace("last_successful_run", last_successful_run)
    config.tables_query = query

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
    if args.fastcount is not None:
        config.fastcount = True

    if args.extract is not None:
        config.extract = args.extract
    if config.target_dataset is not None and "." not in config.target_dataset:
        parser.error("Dataset must be in format project.dataset")

    if config.target_uri is not None and "gs://" not in config.target_uri:
        parser.error(
            f"Loading a dataset requires gs:// URI (uri is {config.target_uri}"
        )

    if config.target_uri is not None and config.target_uri.endswith("/"):
        parser.error(f"target_uri cannot end with /")

    if config.project is not None:
        os.environ["GOOGLE_CLOUD_PROJECT"] = config.project
    if config.credentials is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.credentials

    return config


def main(args=None):

    logger.info("DUMPTY ETL STARTED...")

    config: Config = config_from_args(args)

    logger.info("config.schema: %s", config.schema)
    logger.info("config.project: %s", config.project)
    logger.info("config.target_uri: %s", config.target_uri)
    logger.info("config.target_dataset: %s", config.target_dataset)
    logger.info(
        "config.target_dataset_description: %s", config.target_dataset_description
    )
    logger.info("config.target_dataset_location: %s", config.target_dataset_location)
    logger.info("config.drop_dataset: %s", config.drop_dataset)
    logger.info("config.normalize_schema: %s", config.normalize_schema)
    logger.info("config.extract: %s", config.extract)
    logger.info("config.last_successful_run: %s", config.last_successful_run)
    logger.info("config.tables_query: %s", config.tables_query)
    logger.info("config.tinydb_database_file: %s", config.tinydb_database_file)
    logger.info("config.tinydb_date: %s", config.tinydb_date)
    logger.info("config.log_file: %s", config.log_file)
    logger.info("config.retry: %s", config.retry)
    logger.info("config.reconcile: %s", config.reconcile)
    logger.info("config.fastcount: %s", config.fastcount)
    logger.info("config.progress_bar: %s", config.progress_bar)

    # Default retry for network operations: 2^x * 1 second between each retry, starting with 5s, up to 30s, die after 30 minutes of retries
    # reraise=True places the exception at the END of the stack-trace dump
    retryer = Retrying(
        wait=wait_random_exponential(multiplier=1, min=5, max=30),
        after=after_log(logger, logging.WARNING),
        stop=(
            stop_after_delay(1800) | stop_after_attempt(0 if not config.retry else 999)
        ),
        reraise=True,
        retry=retry_if_not_exception_type(BadRequest),
    )

    # Create spark logdir if needed
    spark_log_dir = config.spark.properties.get("spark.eventLog.dir")
    if spark_log_dir is not None:
        if not os.path.exists(spark_log_dir):
            os.makedirs(spark_log_dir)

    summary = {
        "start_date": datetime.now(),
        "schema": config.schema,
        "tables": [],
        "warnings": [],
    }
    completed: List[Extract] = []

    # Initialize SqlAlchemy
    engine = create_engine(
        config.sqlalchemy.url,
        pool_size=config.introspect_workers,
        connect_args=config.sqlalchemy.connect_args,
        pool_pre_ping=True,
        max_overflow=config.introspect_workers,
        isolation_level=config.sqlalchemy.isolation_level,
        echo=False,
    )

    failed = False

    with ExtractDB(
        config.tinydb_database_file, default_table_name=config.schema
    ) as extract_db:
        with Pipeline(engine, retryer, config) as pipeline:
            with engine.connect() as con:

                # Create destination dataset
                # DO NOT DROP THE SINGLE COPY OF DATASET - drop_dataset: false
                if config.target_dataset is not None:
                    retryer(
                        pipeline.gcp.bigquery_create_dataset,
                        dataset_ref=config.target_dataset,
                        drop=config.drop_dataset,
                        location=config.target_dataset_location,
                        description=config.target_dataset_description,
                        labels=config.target_dataset_pre_labels,
                        access_entries=config.target_dataset_access_entries,
                    )

                if config.reconcile:
                    # Check if tables being requested actually exist in SQL database before doing anything else
                    # This can be very slow for databases with thousands of tables so it is off by default
                    pipeline.reconcile(config.tables)

                """
                STEPS:
                1 - FIRST CREATE A LIST OF TABLES THAT HAD DATA CHANGES (between the last successful ETL execution date and today)
                2 - SECONDLY INTROSPECT AND EXTRACT THE TABLES THAT HAD DATA CHANGES AND LOAD THOSE TABLES INTO BIGQUERY
                3 - UPDATE THE LAST SUCCESSFUL ETL EXECUTION DATE
                """

                logger.info("FIRST CREATE A LIST OF TABLES THAT HAD DATA CHANGES...")

                table_list = config.tables
                logger.info("Total number of tables in YAML: %d", len(table_list))

                if config.extract.strip() == "incremental":

                    logger.info("Running INCREMENTAL EXTRACTION ETL...")

                    query = config.tables_query
                    rs = con.execute(query)
                    rs_all = rs.fetchall()

                    result_list = []
                    for row in rs_all:
                        result_list.append(row[0])

                    logger.info(
                        "Total number of tables that has data changes in CR_STAT_EXTRACT & CR_STAT_DERTBL: %d",
                        len(result_list),
                    )

                    table_list.sort()
                    result_list.sort()

                    tables_to_extract = [
                        value for value in table_list if value in result_list
                    ]

                    logger.info(
                        "Total number of tables with data changes for INCREMENTAL extraction: %d",
                        len(tables_to_extract),
                    )

                elif config.extract.strip() == "full":

                    logger.info("Running FULL EXTRACTION ETL...")

                    tables_to_extract = [value for value in table_list]

                    logger.info(
                        "Total number of tables for FULL extraction: %d",
                        len(tables_to_extract),
                    )

            logger.info("SECONDLY INTROSPECT AND EXTRACT THE TABLES...")

            try:
                # Start a background thread to feed Extract objects to introspect queue
                pipeline.submit([extract_db.get(table) for table in tables_to_extract])

                count = 0
                with alive_bar(
                    len(tables_to_extract),
                    dual_line=True,
                    stats=False,
                    disable=not config.progress_bar,
                ) as bar:
                    while (
                        count < len(tables_to_extract)
                        and pipeline.error_queue.qsize() == 0
                        and not failed
                    ):
                        bar.text = f"| {pipeline.status()} | CPU:{psutil.cpu_percent()}% | Mem:{psutil.virtual_memory()[2]}%"
                        try:
                            extract: Extract = pipeline.done_queue.get(timeout=1)
                            extract_db.save(extract)
                            completed.append(extract)
                            summary["tables"].append(extract.name)
                            if config.target_dataset is not None:
                                if not extract.consistent():
                                    warning = f"{extract.name}: row count mismatch (expected: {extract.rows}, loaded: {extract.rows_loaded}+"
                                    logger.warning(warning)
                                    summary["warnings"].append(warning)
                            count += 1
                            bar()
                        except Empty:
                            pass
                        except KeyboardInterrupt:
                            logger.error("Control-c pressed, shutting down!")
                            pipeline.shutdown()
                            failed = True

                    if pipeline.error_queue.qsize() > 0:
                        pipeline.shutdown()
                        failed = True

            except Exception as e:
                logger.error("\nETL FAILED!!!")
                logger.error("EXCEPTION: ", e)
                pipeline.shutdown()
                failed = True

            if (
                not failed
                and config.target_dataset is not None
                and len(config.target_dataset_post_labels) > 0
            ):
                retryer(
                    pipeline.gcp.bigquery_apply_labels,
                    dataset_ref=config.target_dataset,
                    labels=config.target_dataset_post_labels,
                )

            if (
                not failed
                and config.target_dataset is not None
                and len(config.target_dataset_additional_access_entries) > 0
            ):
                retryer(
                    pipeline.gcp.bigquery_append_access_entries,
                    dataset_ref=config.target_dataset,
                    access_entries=config.target_dataset_additional_access_entries,
                )

            db = TinyDB(config.tinydb_date)
            if not failed:
                # UPDATE THE VALUES IN THE TINY DB DATABASE AFTER A SUCCESSFUL DATA EXTRACTION
                db.upsert(
                    {"value": str(date.today().strftime("%d-%b-%Y"))},
                    Query().name == "last_successful_run",
                )

            last_successful_run = db.get(Query().name == "last_successful_run").get(
                "value"
            )
            # print("\nLAST SUCCESSFUL ETL EXECUTION DATE (AFTER EXTRACT): %s", db.all())
            logger.info(
                "LAST SUCCESSFUL ETL EXECUTION DATE (AFTER EXTRACT): %s",
                last_successful_run,
            )

            db.close()
            logger.info("DATA EXTRACTION IS DONE!!!")

    # Summarize
    summary["end_date"] = datetime.now()
    summary["elapsed_s"] = round(
        (summary["end_date"] - summary["start_date"]).total_seconds()
    )

    if config.target_dataset is not None:
        summary["consistent"] = all(x.consistent() for x in completed)
        summary["bq_bytes"] = sum(
            x.bq_bytes if x.bq_bytes is not None else 0 for x in completed
        )

    if config.target_uri is not None:
        summary["gcs_bytes"] = sum(
            x.gcs_bytes if x.gcs_bytes is not None else 0 for x in completed
        )

    with open(config.log_file, "w") as outfile:
        outfile.write(json.dumps(summary, indent=4, default=str))

    if not len(summary["warnings"]) == 0:
        logger.warning(f"{len(summary['tables'])} tables loaded, with warnings")

    if failed:
        logger.error(
            "DUMPTY ETL FAILED AT: ", datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        )
        exit(1)

    logger.info("DUMPTY ETL ENDED...")


if __name__ == "__main__":
    main()
