from abc import ABC, abstractmethod

from sqlalchemy.engine import Engine
from sqlalchemy.sql.sqltypes import *


class Sql(ABC):
    """Class for handling dialect-specific SQL statements that can't be executed using SqlAlchemy core (query builder)
    Currently, there are none of these, but this class is being left here for future use...
    """

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


class Oracle(Sql):
    def __init__(self, schema: str, engine: Engine):
        super().__init__(schema, engine)

    def get_row_count(self, table: str) -> int:
        sql = f"SELECT COUNT(*) AS count FROM {self.schema}.{table}"
        with self.engine.connect() as connection:
            result = connection.execute(sql).scalar()
        return int(result)
