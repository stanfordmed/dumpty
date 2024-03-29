# Dumpty
**A utility for bulk migration of large on-premise databases to BigQuery** 

[![Apache license](https://img.shields.io/badge/license-apache-brightgreen.svg)](LICENSE.txt)

## About

This utility was created from a need to dump large (> 10 TB) on-premise relational databases into BigQuery. This tool *might* be a good choice for you if:
- Your database tables do not have an auto-incrementing primary key, or a last_updated column, so there is no way to (easily) determine incremental changes.
- Your database is not constantly being updated at unknown times, for example:
    - A reporting server that is updated once-per-day by an ETL with known start/stop times.
    - A restored database that you would like to upload to BigQuery
- You have terabytes of data to upload, only a few hours to do it, and lots of CPU and RAM at your disposal.

When executed, Dumpty will create a "local" (in-memory) Apache Spark server. With a provided configuration and list of tables, it will launch Spark jobs to extract the SQL as compressed JSON streamed directly to a GCS bucket and loaded into BigQuery (if desired). JSON is the only format "officially" supported by this utility as it is the only format which supports the DATETIME type natively in BigQuery.

Again, you are creating a Spark cluster __in memory__, so a host with at least 32 CPU and 32GB of RAM is recommended for large databases. It has been tested against MSSQL Server only, Oracle support is planned. This program can place heavy load on the target database, so ask your DBA/admin for permission first.

## Features

Dumpty will attempt to extract each table using multiple threads by introspecting the primary key. Each thread is assigned a partition from the table and will create a `part-xxx.json.gz` file. 

Based on the type primary key and the row count, it will decide on one of three strategies: 

- **Spark partitioning**
  - The PK is numeric and appears to be evenly distributed (eg. auto-incrementing):
    - The table will be extracted using the `partitionColumn`, `lowerBounds`, `upperBounds`, and `numPartitions` features native to Spark.
- "**Julienne**"
  - The PK is numeric, but badly skewed, or non-numeric:
    - The table will be sliced into partitions of _n_ rows each by taking the row count and dividing it by the number of desired partitions (to determine _n_).
    - The boundaries of each partition are determined by taking the ROW_NUMBER() of each row (ordered by the PK) modulo _n_. This results in partitions of equal size, even if the PK is badly skewed. 
    - This process is inherently slow, so the results are saved to a local database for future runs.
- **No partitioning** 
  - There are fewer than 1,000,000 rows in the table:
    - The table be extracted using a single thread

At first run, the number of partitions is determined by taking `(row count / 1,000,000)`. So 100M rows will be extracted into 100 partitions. After successful extract, the partition size will be re-calculated by summing the compressed JSON output total size and dividing by `target_partition_size_bytes`. 

For example, the first extract for a table with 22M rows will be assigned 22 partitions. If sum total of the `part-*.json.gz` files is less than `target_partition_size_bytes`, then the partition recommendation will be for a single partition for the next run. This will free threads for larger / more complicated tables. Conversely if those 22 partitions created files much larger than `target_partition_size_bytes` the number of partitions will be increased. The size of the final partition file is important as BigQuery loads compressed JSON files using a single thread for each file (unlike Avro & Parquet). We've found 50MB to be a reasonable value for `target_partition_size_bytes`. 

Because introspection is slow, the result of the previous introspection is reused for the next run. Eventually this introspection data will start to drift from the actual state of the database. For this reason the introspection can be set to 'expire' using the configuration parameter `introspection_expire_s`. If the introspection has expired, the table min, max, and partitioning will be recalculated. Note there should never be any loss of data from using 'old' introspection data, it just means (typically) that the last partition will continously grow in size while the other remain the same size. 

For very large databases, you will want to extract the data once to determine the correct partitioning sizes, and again to extract with the new partition sizing. Subsequent executions will use the new partition sizing unless it is set to expire. 

A simple database (`tinydb.json`) contains the introspection data. Deleting this file will trigger re-introspection of the database. Running two or more instances of Dumpty can/will result in corruption of `tinydb.json`. 

## Installation

- Create a new Python 3.9.x virtual-env (recommended)
- Clone this repo
- To install from current directory (eg. during development)
  - `pip install build`
  - Install from cwd: `pip install -e .`
  - This will create a `dumpty` command line application which will directly run `src/dumpty/main.py`
- You can also create a .tar.gz dist package, which can be installed anywhere:
  - `pip install build`
  - `python -m build` 
  - Copy `dist/dumpty-0.1.0.tar.gz` to your remote server and install with `pip install dumpty-0.1.0.tar.gz`
- You will need to download Spark JAR files (not included here) for your database and GCP platform (see `spark.driver.extraClassPath` in [config.yaml.example](config.yaml.example))
- Java 11+ is recommended for performance reasons

## Usage

### Configuration

Dumpty uses Jinja-templated YAML configuration files. This allows breaking up the configuration file into multiple files, which can all import shared configuration.

The following is an example database.yaml configuration file, which imports a common config.yaml which contains Spark and database configuration. The template uses Jinja formatting and environment variables are available: 

```yaml
{% include "config.yaml" %}
{%- set THREADS = env['THREADS'] %}
spark:
  threads: {{THREADS}}
  format: "json"
  compression: "gzip"
  normalize_schema: "true"
project: my-gcp-project
credentials: /path/to/gcp/credentials.json
target_uri: gs://my_bucket/database_name
target_dataset: my-gcp-project.my-extracted-dataset
target_partition_size_bytes: 52428800 # 50 MiB is our target *.json.gz size
introspection_expire_s: 604800 # re-introspect tables after 7 days
schema: dbo
tables:
  - REPORTING
  - SCHEDULE
  - AUDIT
```

For a complete example, see [config.yaml.example](config.yaml.example)

# Misc notes:
## Mac M1 development

Getting this working natively on M1 Mac with Python and PyODBC is a bit of a challenge. Note you may not even __need__ PyODBC (for example, Oracle uses [cx_Oracle](https://oracle.github.io/python-cx_Oracle/)). 

In short you want to make sure your PyODBC does __not__ link to iodbc:
- `otool -L $(python3 -c "import importlib.machinery; print(importlib.machinery.PathFinder().find_spec('pyodbc').origin)")`
  - Make sure iodbc is not mentioned in the output

## Installation 

### Homebrew
- `brew install unixodbc`
  - Follow directions [here](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16)
   - `sudo ln -s /opt/homebrew/etc/odbcinst.ini /etc/odbcinst.ini`
   - `sudo ln -s /opt/homebrew/etc/odbc.ini /etc/odbc.ini`

### Build and install PyODBC

This assumes you are using [pyenv](https://github.com/pyenv/pyenv) to install Python

```sh
bash$ wget https://files.pythonhosted.org/packages/2c/93/1468581e505e4e6611064ca9d0cebb93b3080133e4363054fdd658e5fff3/pyodbc-4.0.35.tar.gz
bash$ export LDFLAGS="-L/opt/homebrew/Cellar/unixodbc/2.3.11/lib -L/opt/homebrew/Cellar/openssl@1.1/1.1.1o/lib"
bash$ export CPPFLAGS="-I/opt/homebrew/Cellar/unixodbc/2.3.11/include -I/opt/homebrew/Cellar/openssl@1.1/1.1.1o/include"
bash$ export CFLAGS="-I/opt/homebrew/Cellar/openssl@1.1/1.1.1o/include/openssl"
pyenv install 3.9.11
pip install pyodbc-4.0.35.tar.gz
```
