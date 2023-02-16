from typing import Tuple
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference, LoadJob, Client as BigqueryClient, SourceFormat, LoadJobConfig, CreateDisposition, WriteDisposition, SchemaField
from google.cloud.exceptions import NotFound
from google.cloud.storage import Blob, Bucket, Client as StorageClient
from urllib.parse import urlparse
import re
import logging
import os
from pathlib import PurePath
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
    client = BigqueryClient()
    exists = False
    ref = DatasetReference.from_string(dataset_ref)
    try:
        dataset_ref: Dataset = client.get_dataset(ref)
        if drop:
            logging.info(f"Dropping dataset {dataset_ref.dataset_id}")
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
        logging.info(f"Updating dataset {dataset_ref.dataset_id}")
        return client.update_dataset(dataset_ref, fields=["description", "location", "labels"])
    else:
        logging.info(f"Creating dataset {dataset_ref.dataset_id}")
        return client.create_dataset(dataset_ref)


def bigquery_load(uri: str, table: str, format: str, schema: list[dict], description: str = None, location="US"):
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

    load_job = client.load_table_from_uri(
        uri, table, job_config=job_config, location=location)

    load_job.result()

    return load_job.output_rows, load_job.output_bytes
