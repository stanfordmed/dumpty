import pandas as pd
import re
from decimal import Decimal
from typing import Tuple, List
from sqlalchemy import Column, MetaData, Table, inspect
from sqlalchemy.engine import Engine, Dialect, Inspector
from abc import ABC, abstractmethod
from dumpty import logger


class Sql(ABC):

    def __init__(self, schema: str, engine: Engine):
        self.engine = engine
        self.schema = schema
        super().__init__()

    @abstractmethod
    def get_row_count(self, table: str) -> int:
        """
        Returns number of rows in table, and number of seconds to get that count
        """
        pass

    @abstractmethod
    def get_schema(self, table: str) -> list[dict]:
        """
        Returns list of JSON schema fields (BigQuery format)
        """
        pass

    @abstractmethod
    def get_min_max_count(self, table: str, column: str) -> Tuple[int, int]:
        """
        Returns the min, max values of a table column along with total row count
        """
    pass

    @abstractmethod
    def julienne(self, table: str, column: str, slices: int):
        """
        Slices a table into equal widths based on the row_number, not PK value. 
        Useful for tables that are badly skewed or contain a non-numeric PK for splitting.
        """
    pass

    def get_pk(self, table: str) -> Column:
        """
        Returns the (first) primary key of a table or None if no PK
        """
        metadata = MetaData(bind=self.engine, schema=self.schema)
        metadata.reflect(schema=self.schema, only=[table])
        table: Table = metadata.tables[f"{self.schema}.{table}"]
        if table.primary_key and len(table.primary_key.columns) > 0:
            return table.primary_key.columns[0]
        else:
            return None
    pass

    def get_tables(self) -> List[str]:
        """
        Returns a sorted list of the table names in a schema (excludes views)
        """
        logger.debug(f"Introspecting schema {self.schema}")
        inspector: Inspector = inspect(self.engine)
        return sorted(inspector.get_table_names(schema=self.schema))


def factory(schema: str, engine: Engine) -> Sql:
    if engine.dialect.name == "mssql":
        return Mssql(schema, engine)
    elif engine.dialect.name == "oracle":
        return Oracle(schema, engine)
    else:
        raise Exception(
            f"Unsupported dialect {engine.dialect.name}")


class Mssql(Sql):
    def __init__(self, schema: str, engine: Engine):
        super().__init__(schema, engine)

    def get_row_count(self, table: str) -> int:
        sql = f"SELECT COUNT(*) AS count FROM [{self.schema}].[{table}]"
        with self.engine.connect() as connection:
            result = connection.execute(sql).scalar()
        return int(result)

    def get_min_max_count(self, table: str, column: str) -> Tuple[int, int]:
        sql = f"SELECT min({column}) AS mn, max({column}) AS mx, COUNT({column}) AS cnt FROM [{self.schema}].[{table}]"
        with self.engine.connect() as connection:
            result = connection.execute(sql).first()
            min = result[0]
            max = result[1]
            if isinstance(min, Decimal):
                min = int(min)
            if isinstance(max, Decimal):
                max = int(max)
            return (min, max, result[2])

    def get_schema(self, table: str) -> list[dict]:
        sql = f"""
                WITH mapping AS (
                SELECT
                    ordinal_position AS column_position,
                    lower(column_name) AS column_name,
                    CASE
                        WHEN is_nullable = 'YES' THEN 'nullable'
                        ELSE 'required'
                    END AS column_mode,
                    CASE
                        WHEN data_type IN ('smallint', 'tinyint', 'int', 'bigint') THEN 'INT64'
                        WHEN data_type IN ('datetime','smalldatetime') THEN 'DATETIME'
                        WHEN data_type = 'date' THEN 'DATE'
                        WHEN data_type = 'uniqueidentifier' THEN 'STRING'
                        WHEN data_type IN ('float','real') THEN 'FLOAT64'
                        WHEN data_type IN ('char', 'nchar', 'varchar', 'nvarchar', 'text') THEN 'STRING'
                        WHEN data_type = 'bit' THEN 'BOOL'
                        WHEN data_type IN ('binary','varbinary') THEN 'BYTES'
                        WHEN data_type = 'datetime2' THEN
                            CASE
                                WHEN numeric_precision BETWEEN 0 AND 6 THEN 'DATETIME'
                                WHEN numeric_precision IS NULL THEN 'DATETIME'
                                WHEN numeric_precision = 7 THEN 'STRING'
                            END
                        WHEN data_type = 'datetimeoffset' THEN
                            CASE
                                WHEN numeric_precision BETWEEN 0 AND 6 THEN 'TIMESTAMP'
                                WHEN numeric_precision IS NULL THEN 'TIMESTAMP'
                                WHEN numeric_precision = 7 THEN 'STRING'
                            END
                        WHEN data_type = 'numeric' OR data_type = 'decimal' THEN
                            CASE
                               WHEN (numeric_scale = 0) AND (numeric_precision <= 18) THEN 'INT64'
                               WHEN (numeric_scale BETWEEN 0 AND 9) AND (
                                       ((CASE WHEN numeric_scale > 0 THEN numeric_scale ELSE 1 END) <= numeric_precision)
                                       AND (numeric_precision <= numeric_scale + 29)
                                   ) THEN 'NUMERIC'
                               WHEN (numeric_scale BETWEEN 0 AND 38) AND (
                                       ((CASE WHEN numeric_scale > 0 THEN numeric_scale ELSE 1 END) <= numeric_precision)
                                       AND (numeric_precision <= numeric_scale + 38)
                                   ) THEN 'BIGNUMERIC'
                            END
                        ELSE 'UNKNOWN_DATA_TYPE_' + data_type
                    END AS column_type,
                    numeric_precision AS numeric_precision,
                    numeric_scale AS numeric_scale
                    FROM information_schema.columns
                    WHERE table_catalog=DB_NAME() AND table_schema='{self.schema}' AND table_name='{table}'
                )
                SELECT
                    column_name,
                    column_mode,
                    column_type,
                    IIF(column_type IN ('NUMERIC', 'BIGNUMERIC'), numeric_precision, null) AS numeric_precision,
                    IIF(column_type IN ('NUMERIC', 'BIGNUMERIC'), numeric_scale, null) AS numeric_scale
                FROM mapping
                ORDER BY column_position;
        """

        conn = self.engine.connect()
        df = pd.read_sql_query(sql, conn)
        conn.close()

        # Remove any special chars from column names
        df['column_name'] = df['column_name'].map(
            lambda x: re.sub(r"[^a-zA-Z0-9]", "_", x).lower())

        df = df.rename(columns={
            'column_name': 'name',
            'column_mode': 'mode',
            'column_type': 'type',
            'numeric_precision': 'precision',
            'numeric_scale': 'scale'
        })

        # Removes precision and scale keys if they are null
        return [row.dropna().to_dict()
                for index, row in df.iterrows()]

    def julienne(self, table: str, column: str, width: int):
        logger.debug(
            f"Julienning table {table} on {column} into {width}-row slices")
        sql = f"""--sql
            SELECT {column}
            FROM (
                SELECT {column}, ROW_NUMBER() OVER(ORDER BY {column}) AS row_num
                FROM {self.schema}.{table}
            ) slices
            WHERE row_num % {width} = 0
            ORDER BY {column}
        """
        conn = self.engine.connect()
        rs = conn.execute(sql)
        results = [r[0] for r in rs]
        conn.close()
        return results


class Oracle(Sql):
    def __init__(self, schema: str, engine: Engine):
        super().__init__(schema, engine)

    def get_row_count(self, table: str) -> int:
        sql = f"SELECT COUNT(*) AS count FROM {self.schema}.{table}"
        with self.engine.connect() as connection:
            result = connection.execute(sql).scalar()
        return int(result)

    def get_min_max_count(self, table: str, column: str) -> Tuple[int, int]:
        sql = f"SELECT min({column}) AS mn, max({column}) AS mx, COUNT({column}) AS cnt FROM {self.schema}.{table}"
        with self.engine.connect() as connection:
            result = connection.execute(sql).first()
            min = result[0]
            max = result[1]
            logger.debug(f"table: {table} min Type: {min.__class___}")
            logger.debug(f"table: {table} max Type: {max.__class___}")
            if isinstance(min, Decimal):
                min = int(min)
            if isinstance(max, Decimal):
                max = int(max)
            return (min, max, result[2])

    def get_schema(self, table: str) -> list[dict]:
        sql = f"""
                WITH mapping AS (
                  SELECT
                      column_id                                     AS column_position,
                      lower(column_name)                            AS column_name,
                      DECODE(nullable, 'Y', 'nullable', 'required') AS column_mode,
                      CASE
                          WHEN data_type = 'DATE' THEN 'DATETIME'
                          WHEN data_type IN ('BINARY_FLOAT','BINARY_DOUBLE','FLOAT') THEN 'FLOAT64'
                          WHEN data_type IN ('NCHAR','NCLOB', 'CHAR', 'CLOB', 'INTERVALDAYTOSECOND',
                                             'INTERVALYEARTOMONTH', 'NVARCHAR2', 'VARCHAR2') THEN 'STRING'
                          WHEN data_type IN ('BLOB','RAW') THEN 'BYTES'
                          WHEN data_type = 'TIMESTAMP' THEN 'DATETIME'
                          WHEN data_type = 'TIMESTAMP WITHLOCALTIMEZONE' THEN 'TIMESTAMP'
                          WHEN data_type = 'TIMESTAMP WITHTIMEZONE' THEN 'TIMESTAMP'
                          WHEN data_type = 'BINARY_DOUBLE' OR data_type = 'NUMBER' THEN
                              CASE
                               WHEN (data_scale = 0) AND (data_precision <= 18) THEN 'INT64'
                               WHEN (data_scale BETWEEN 0 AND 9) AND (
                                       ((CASE WHEN data_scale > 0 THEN data_scale ELSE 1 END) <= data_precision)
                                       AND (data_precision <= data_scale + 29)
                                   ) THEN 'NUMERIC'
                               WHEN (data_scale BETWEEN 0 AND 38) AND (
                                       ((CASE WHEN data_scale > 0 THEN data_scale ELSE 1 END) <= data_precision)
                                       AND (data_precision <= data_scale + 38)
                                   ) THEN 'BIGNUMERIC'
                              END
                          ELSE 'UNKNOWN_DATA_TYPE_' || data_type
                      END AS column_type,
                      data_precision AS numeric_precision,
                      data_scale AS numeric_scale
                      FROM all_tab_cols
                      WHERE owner = '{self.schema}' AND LOWER(TABLE_NAME)='{table}'
                  )
              SELECT
                  column_name,
                  column_mode,
                  column_type,
                  CASE WHEN column_type IN ('NUMERIC', 'BIGNUMERIC') THEN numeric_precision END AS numeric_precision,
                  CASE WHEN column_type IN ('NUMERIC', 'BIGNUMERIC') THEN numeric_scale END AS numeric_scale
              FROM mapping
              ORDER BY column_position
        """

        conn = self.engine.connect()
        df = pd.read_sql_query(sql, conn)
        conn.close()

        # Remove any special chars from column names
        df['column_name'] = df['column_name'].map(
            lambda x: re.sub(r"[^a-zA-Z0-9]", "_", x).lower())

        df = df.rename(columns={
            'column_name': 'name',
            'colum_mode': 'mode',
            'column_type': 'type',
            'numeric_precision': 'precision',
            'numeric_scale': 'scale'
        })

        # Removes precision and scale keys if they are null
        return [row.dropna().to_dict()
                for index, row in df.iterrows()]
