from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from dumpty import logger
from dumpty.config import Config
from dumpty.introspector import Extract
from dumpty.util import normalize_str


class LocalSpark:
    """Class for extracting tables using Spark
    """

    def __init__(self, config: Config, output_uri: str):
        self._spark_config = config.spark
        self._jdbc_config = config.jdbc
        self._output_uri = output_uri
        self._spark_session: SparkSession = None

    def __enter__(self):
        ctx = SparkSession\
            .builder\
            .master(f'local[{self._spark_config.threads}]')\
            .appName('Dumpty')\
            .config(conf=SparkConf().setAll(list(self._spark_config.properties.items())))
        self._spark_session = ctx.getOrCreate()
        self._spark_session.sparkContext.setLogLevel(
            self._spark_config.log_level)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._spark_session.stop()

    @staticmethod
    def normalize_df(df: DataFrame) -> DataFrame:
        return df.select([col(x).alias(normalize_str(x)) for x in df.columns])

    def full_extract(self, extract: Extract, uri: str) -> str:
        session = self._spark_session
        # spark.sparkContext.setJobGroup(table.name, "full extract")
        if extract.predicates is not None:
            session.sparkContext.setJobDescription(
                f'{extract.name} ({len(extract.predicates)} predicates)')
            df = session.read.jdbc(
                self._jdbc_config.url,
                table=extract.name,
                predicates=extract.predicates,
                properties=self._jdbc_config.properties
            )
        elif extract.partition_column is not None and min is not None and max is not None:
            session.sparkContext.setJobDescription(
                f'{extract.name} (partitioned on [{extract.partition_column}] from {extract.min} to {extract.max})')
            df = session.read.jdbc(
                url=self._jdbc_config.url,
                table=extract.name,
                column=extract.partition_column,
                lowerBound=str(extract.min),
                upperBound=str(extract.max),
                numPartitions=extract.partitions,
                properties=self._jdbc_config.properties
            )
        else:
            # Simple table dump
            session.sparkContext.setJobDescription(
                f'{extract.name}')
            df = session.read.jdbc(
                url=self._jdbc_config.url,
                table=extract.name,
                properties=self._jdbc_config.properties
            )

        # Always normalize table name
        n_table_name = normalize_str(extract.name)
        if self._spark_config.normalize_schema:
            # Normalize column names?
            df = self.normalize_df(df)

        # Attempted different methods of attaching listeners to get row count, none were reliable

        session.sparkContext.setLocalProperty("callSite.short", n_table_name)

        logger.debug(f"Extracting {extract.name} as {n_table_name}")
        df.write.save(f"{uri}/{n_table_name}", format=self._spark_config.format, mode="overwrite",
                      timestampFormat=self._spark_config.timestamp_format, compression=self._spark_config.compression)

        final_uri = f"{uri}/{n_table_name}/part-*.{self._spark_config.format.lower()}" + \
            (".gz" if self._spark_config.compression == "gzip" else "")

        return final_uri
