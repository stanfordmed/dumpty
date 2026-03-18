import os
import re
from pathlib import PurePath
from urllib.parse import urlparse

import google.api_core.exceptions as core_exceptions
from google.cloud.bigquery import AccessEntry
from google.cloud.bigquery import Client as BigqueryClient
from google.cloud.bigquery import CreateDisposition
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import DatasetReference
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery import QueryPriority
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import SourceFormat
from google.cloud.bigquery import Table
from google.cloud.bigquery import TableReference
from google.cloud.bigquery import WriteDisposition
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.exceptions import NotFound
from google.cloud.storage import Blob
from google.cloud.storage import Bucket
from google.cloud.storage import Client as StorageClient

from dumpty import logger


class GCP:
    """GCP helper functions using a shared client instance"""

    def __init__(self):
        """Initialize a :class:`.GCP` instance."""
        self._storage_client = StorageClient()
        self._bigquery_client = BigqueryClient()

    def get_size_bytes(self, uri: str) -> int:
        """
        Returns size (bytes) of all objects matching a GCS prefix or glob pattern
        """
        parse = urlparse(uri)
        bucket = parse.netloc
        path = parse.path
        if "*" in path:
            prefix = os.path.dirname(path).lstrip("/") + "/"
            glob = os.path.basename(path)
        else:
            prefix = path.lstrip("/")
            glob = None
        blobs = self._storage_client.list_blobs(bucket, prefix=prefix)
        blob: Blob
        bytes = 0
        for blob in blobs:
            if glob and "*" in glob:
                if blob.name and PurePath(blob.name).match(glob):
                    bytes += blob.size or 0
            else:
                bytes += blob.size or 0
        return bytes

    def upload_from_string(self, data: str, uri: str, content_type="application/json"):
        """
        Writes a string to a URI eg. gs://bucket/name (defaults to application/json content type)
        """
        matches = re.match("gs://(.*?)/(.*)", uri)
        if matches:
            bucket_name, name = matches.groups()
        else:
            raise Exception(f"Invalid GCS URI {uri}")
        bucket: Bucket = self._storage_client.bucket(bucket_name)
        blob: Blob = bucket.blob(name)
        blob.upload_from_string(data=data, content_type=content_type)

    def bigquery_create_dataset(
        self,
        dataset_ref: str,
        description: str | None = None,
        location: str = "US",
        labels: dict | None = None,
        access_entries: list[dict] | None = None,
        drop: bool = False,
    ) -> Dataset:
        """
        Creates a Dataset in BigQuery
        """
        if labels is None:
            labels = {}
        logger.info(f"Using dataset {dataset_ref}")
        exists = False
        ref = DatasetReference.from_string(dataset_ref)
        try:
            dataset: Dataset = self._bigquery_client.get_dataset(ref)
            if drop:
                logger.info(f"Dropping dataset {dataset.dataset_id}")
                self._bigquery_client.delete_dataset(dataset, not_found_ok=True, delete_contents=True)
                dataset = Dataset(ref)
            else:
                exists = True
        except NotFound:
            dataset = Dataset(ref)

        dataset.description = description
        dataset.location = location
        dataset.labels = labels

        if access_entries is not None:
            updated_entries = list(dataset.access_entries) if dataset.access_entries is not None else []
            for entry in access_entries:
                ae = AccessEntry.from_api_repr(entry)
                if ae not in updated_entries:
                    updated_entries.append(ae)
            dataset.access_entries = updated_entries

        if exists:
            logger.debug(f"Dataset {dataset.dataset_id} already exists, updating")
            if access_entries is not None:
                return self._bigquery_client.update_dataset(
                    dataset, fields=["description", "location", "labels", "access_entries"]
                )
            return self._bigquery_client.update_dataset(dataset, fields=["description", "location", "labels"])
        else:
            return self._bigquery_client.create_dataset(dataset)

    def bigquery_create_table(
        self, table_ref: str, schema: list[dict], description: str | None = None, labels: dict | None = None
    ) -> Table:
        """
        Creates a Table in BigQuery
        """
        if labels is None:
            labels = {}
        table = Table(TableReference.from_string(table_ref), schema)
        table.description = description
        table.labels = labels
        logger.info(f"Creating empty table {table.dataset_id}.{table.table_id}")
        return self._bigquery_client.create_table(table, exists_ok=True)

    def bigquery_create_view(self, view_ref: str, sql: str, materialized: bool = False):
        """
        Creates a view in BigQuery
        """
        view: Table = Table(view_ref)
        if materialized:
            view.mview_query = sql
        else:
            view.view_query = sql
        try:
            return self._bigquery_client.create_table(view)
        except core_exceptions.Conflict:
            self._bigquery_client.delete_table(view, not_found_ok=True)
            return self._bigquery_client.create_table(view)
        except Exception as e:
            logger.error(f"Exception creating view ${view} sql: ${sql}")
            raise e

    def bigquery_apply_labels(self, dataset_ref: str, labels: dict):
        ref = DatasetReference.from_string(dataset_ref)
        dataset = self._bigquery_client.get_dataset(ref)
        dataset.labels = labels
        logger.debug(f"Updating labels on dataset {dataset_ref}")
        dataset = self._bigquery_client.update_dataset(dataset, ["labels"])

    def bigquery_append_access_entries(self, dataset_ref: str, access_entries: list[dict]):
        """Appends a list of access entries to an existing BigQuery dataset.

        Args:
            dataset_ref (str): project_id.dataset_id reference to dataset
            access_entries (List[dict]): List of AccessEntry objects
        """
        ref = DatasetReference.from_string(dataset_ref)
        dataset = self._bigquery_client.get_dataset(ref)
        updated_entries = list(dataset.access_entries)

        for entry in access_entries:
            ae = AccessEntry.from_api_repr(entry)
            if ae not in dataset.access_entries:
                updated_entries.append(ae)

        dataset.access_entries = updated_entries
        logger.debug(f"Appended new access entries to dataset {dataset_ref}")
        dataset = self._bigquery_client.update_dataset(dataset, ["access_entries"])

    def bigquery_load(
        self, uri: str, table: str, format: str, schema: list[dict], description: str | None = None, location="US"
    ):
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
            raise Exception(f"Unknown format {format}")

        try:
            # Update description of the table if it exists (setting this in LoadJobConfig() below gives an exception when we do not drop and recreate the dataset)
            table_exists = self._bigquery_client.get_table(table)
            table_exists.description = description
            table_exists = self._bigquery_client.update_table(table_exists, ["description"])
        except NotFound:
            pass
        job_config = LoadJobConfig(
            schema=[SchemaField.from_api_repr(field) for field in schema],
            source_format=source_format,
            create_disposition=CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )

        # Some tables may take longer to load than the default deadline of 600s
        load_job = self._bigquery_client.load_table_from_uri(
            uri, table, retry=DEFAULT_RETRY.with_timeout(1800), job_config=job_config, location=location
        )
        load_job.result()

        return load_job.output_rows, load_job.output_bytes

    def bigquery_run_query(
        self,
        sql: str,
        destination_table: TableReference,
        write_disposition: str = "CREATE_IF_NEEDED",
        drop: bool = False,
    ):
        """Run query in BigQuery."""
        job_config = QueryJobConfig(priority=QueryPriority.BATCH)
        if destination_table is not None:
            if drop:
                self._bigquery_client.delete_table(destination_table, not_found_ok=True)
            job_config.destination = destination_table
            job_config.write_disposition = write_disposition
        try:
            query_job = self._bigquery_client.query(sql, job_config=job_config)
            return query_job.result()
        except Exception as e:
            raise e
