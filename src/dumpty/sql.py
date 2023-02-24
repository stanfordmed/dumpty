from sqlalchemy import Column, Table
from sqlalchemy.sql.sqltypes import *

from dumpty import logger
from dumpty.util import normalize_str


def bq_schema(table: Table) -> list[dict]:
    """
    Returns Table schema in BigQuery JSON schema format
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
        elif isinstance(col.type, (Numeric)):
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
            logger.warning(
                f"Unmapped type in {table.name}.{col.name} ({col.type}), defaulting to STRING")
            schema['type'] = "STRING"

        schemas.append(schema)
    return schemas
