import os
import re
from pathlib import PurePath
from typing import List
from urllib.parse import urlparse

from google.cloud.bigquery import Client as BigqueryClient
from google.cloud.bigquery import (CreateDisposition, Dataset,
                                   DatasetReference, LoadJobConfig, TableReference, Table,
                                   SchemaField, SourceFormat, WriteDisposition)
from google.cloud.exceptions import NotFound
from google.cloud.storage import Blob, Bucket
from google.cloud.storage import Client as StorageClient
from google.cloud.bigquery.retry import DEFAULT_RETRY
from dumpty import logger


def get_size_bytes(uri: str) -> int:
    """
    Returns size (bytes) of all objects matching a GCS prefix or glob pattern
    """
    parse = urlparse(uri)
    client: StorageClient = StorageClient()
    bucket = parse.netloc
    path = parse.path
    if "*" in path:
        prefix = os.path.dirname(path).lstrip("/") + "/"
        glob = os.path.basename(path)
    else:
        prefix = path.lstrip("/")
        glob = None
    blobs = client.list_blobs(bucket, prefix=prefix)
    blob: Blob
    bytes = 0
    for blob in blobs:
        if glob and "*" in glob:
            if PurePath(blob.name).match(glob):
                bytes += blob.size
        else:
            bytes += blob.size
    return bytes


def upload_from_string(data: str, uri: str, content_type="application/json"):
    """
    Writes a string to a URI eg. gs://bucket/name (defaults to application/json content type)
    """
    matches = re.match("gs://(.*?)/(.*)", uri)
    if matches:
        bucket, name = matches.groups()
    else:
        raise Exception(f"Invalid GCS URI {uri}")
    client: StorageClient = StorageClient()
    bucket: Bucket = client.bucket(bucket)
    blob: Blob = bucket.blob(name)
    blob.upload_from_string(data=data, content_type=content_type)


def bigquery_create_dataset(dataset_ref: str, description: str = None, location: str = "US", labels: dict = {}, drop: bool = False) -> Dataset:
    """
    Creates a Dataset in BigQuery
    """
    logger.info(f"Using dataset {dataset_ref}")
    client = BigqueryClient()
    exists = False
    ref = DatasetReference.from_string(dataset_ref)
    try:
        dataset_ref: Dataset = client.get_dataset(ref)
        if drop:
            logger.info(f"Dropping dataset {dataset_ref.dataset_id}")
            client.delete_dataset(
                dataset_ref, not_found_ok=True, delete_contents=True)
        else:
            exists = True
    except NotFound:
        dataset_ref: Dataset = Dataset(ref)
    dataset_ref.description = description
    dataset_ref.location = location
    dataset_ref.labels = labels
    if exists:
        logger.debug(
            f"Dataset {dataset_ref.dataset_id} already exists, updating")
        return client.update_dataset(dataset_ref, fields=["description", "location", "labels"])
    else:
        return client.create_dataset(dataset_ref)


def bigquery_create_table(table_ref: str, schema: List[dict], description: str = None, labels: dict = {}) -> Table:
    """
    Creates a Table in BigQuery
    """
    client = BigqueryClient()
    table_ref: Table = Table(TableReference.from_string(table_ref), schema)
    table_ref.description = description
    table_ref.labels = labels
    logger.info(
        f"Creating empty table {table_ref.dataset_id}.{table_ref.table_id}")
    return client.create_table(table_ref, exists_ok=True)


def bigquery_load(uri: str, table: str, format: str, schema: List[dict], description: str = None, location="US"):
    """
    Loads a dataset into BigQuery from GCS bucket
    """
    if format == "json":
        source_format = SourceFormat.NEWLINE_DELIMITED_JSON
    elif format == "csv":
        source_format = SourceFormat.CSV
    elif format == "parquet":
        source_format = SourceFormat.PARQUET
    elif format == "orc":
        source_format = SourceFormat.ORC
    else:
        raise Exception("Unknown format {}".format(format))

    client = BigqueryClient()
    job_config = LoadJobConfig(
        schema=[SchemaField.from_api_repr(field)
                for field in schema],
        source_format=source_format,
        create_disposition=CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        destination_table_description=description
    )

    # Some tables may take longer to load than the default deadline of 600s
    load_job = client.load_table_from_uri(uri, table, retry=DEFAULT_RETRY.with_timeout(
        1800), job_config=job_config, location=location)
    load_job.result()

    return load_job.output_rows, load_job.output_bytes
