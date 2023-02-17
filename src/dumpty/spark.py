from pyspark.sql import SparkSession
from pyspark import SparkConf
from dumpty.config import Config
from dumpty.util import normalize_df, normalize_str
from tinydb.table import Document
from dumpty import logger


class LocalSpark:

    def __init__(self, config: Config, output_uri: str):
        self.config = config.spark
        self.jdbc = config.jdbc
        self.output_uri = output_uri
        self.context: SparkSession = None

    def __enter__(self):
        ctx = SparkSession\
            .builder\
            .master(f'local[{self.config.threads}]')\
            .appName('Dumpty')\
            .config(conf=SparkConf().setAll(list(self.config.properties.items())))
        self.context = ctx.getOrCreate()
        self.context.sparkContext.setLogLevel(self.config.log_level)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.context.stop()

    def full_extract(self, table: Document, uri: str) -> str:
        spark = self.context
        table_name = table['name']
        partitions = table.get('partitions')
        predicates = table.get('predicates')
        partition_column = table.get('partition_column')
        min = table.get('min')
        max = table.get('max')
        # spark.sparkContext.setJobGroup(table.name, "full extract")
        if predicates is not None:
            spark.sparkContext.setJobDescription(
                f'{table_name} ({len(predicates)} predicates)')
            df = spark.read.jdbc(
                self.jdbc.url,
                table=table_name,
                predicates=predicates,
                properties=self.jdbc.properties
            )
        elif partition_column is not None and min is not None and max is not None:
            spark.sparkContext.setJobDescription(
                f'{table_name} (partitioned on [{partition_column}] from {min} to {max})')
            df = spark.read.jdbc(
                url=self.jdbc.url,
                table=table_name,
                column=partition_column,
                lowerBound=str(min),
                upperBound=str(max),
                numPartitions=partitions,
                properties=self.jdbc.properties
            )
        else:
            # Simple table dump
            spark.sparkContext.setJobDescription(
                f'{table_name}')
            df = spark.read.jdbc(
                url=self.jdbc.url,
                table=table_name,
                properties=self.jdbc.properties
            )

        # Always normalize table name
        n_table_name = normalize_str(table_name)
        if self.config.normalize_schema:
            # Normalize column names?
            df = normalize_df(df)

        # Attempted different methods of attaching listeners to get row count, none were reliable

        spark.sparkContext.setLocalProperty("callSite.short", n_table_name)

        logger.debug(f"Extracting {table_name} as {n_table_name}")
        df.write.save(f"{uri}/{n_table_name}", format=self.config.format, mode="overwrite",
                      timestampFormat=self.config.timestamp_format, compression=self.config.compression)

        final_uri = f"{uri}/{n_table_name}/part-*.{self.config.format.lower()}" + \
            (".gz" if self.config.compression == "gzip" else "")

        return final_uri
