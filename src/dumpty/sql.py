import re
from decimal import Decimal
from typing import Tuple, List
from sqlalchemy import Column, MetaData, Table, inspect
from sqlalchemy.engine import Engine, Dialect, Inspector
from sqlalchemy.sql.sqltypes import *
from abc import ABC, abstractmethod
from dumpty import logger
from dumpty.util import normalize_str


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

    def get_schema(self, table: Table) -> list[dict]:
        """
        Returns list of BigQuery JSON schema fields
        """
        col: Column
        schemas = []
        for col in table.columns:
            schema = {}
            schema['name'] = normalize_str(col.name)
            schema['mode'] = "Nullable" if col.nullable else "Required"

            if isinstance(col.type, (SmallInteger, Integer, BigInteger)):
                schema['type'] = "INT64"
            elif isinstance(col.type, (DateTime)):
                schema['type'] = "DATETIME"
            elif isinstance(col.type, (Date)):
                schema['type'] = "DATE"
            elif isinstance(col.type, (Float, REAL)):
                schema['type'] = "FLOAT64"
            elif isinstance(col.type, (String)):
                schema['type'] = "STRING"
            elif isinstance(col.type, (Boolean)):
                schema['type'] = "BOOL"
            elif isinstance(col.type, (LargeBinary)):
                schema['type'] = "BYTES"
            elif isinstance(col.type, (Numeric, Decimal)):
                p = col.type.precision
                s = col.type.scale
                if s == 0 and p <= 18:
                    schema['type'] = "INT64"
                elif (s >= 0 and s <= 9) and ((max(s, 1) <= p) and p <= s+29):
                    schema['type'] = "NUMERIC"
                    schema['precision'] = p
                    schema['scale'] = s
                elif (s >= 0 and s <= 38) and ((max(s, 1) <= p) and p <= s+38):
                    schema['type'] = "BIGNUMERIC"
                    schema['precision'] = p
                    schema['scale'] = s
            else:
                logger.error(
                    f"Unmapped type in {table.name}.{col.name} ({col.type}), defaulting to STRING")
                schema['type'] = "STRING"

            schemas.append(schema)
        return schemas

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

    def introspect_table(self, table: str) -> Table:
        """
        Introspects the database for a table
        """
        logger.debug(f"Introspecting {table}")
        metadata = MetaData(bind=self.engine, schema=self.schema)
        return Table(table, metadata, autoload=True)

    def get_tables(self) -> List[str]:
        """
        Returns a sorted list of the table names in a schema (excludes views)
        """
        logger.debug(f"Enumerating tables in schema {self.schema}")
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
        sql = f"SELECT min([{column}]) AS mn, max([{column}]) AS mx, COUNT([{column}]) AS cnt FROM [{self.schema}].[{table}]"
        with self.engine.connect() as connection:
            result = connection.execute(sql).first()
            min = result[0]
            max = result[1]
            if isinstance(min, Decimal):
                min = int(min)
            if isinstance(max, Decimal):
                max = int(max)
            return (min, max, result[2])

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
