import random
import re
import datetime
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql import sqltypes
from sqlalchemy import literal_column


def normalize_str(x: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", x).lower()


def filter_shuffle(seq):
    """
    Filter for Jinja to shuffle a list
    """
    try:
        result = list(seq)
        random.shuffle(result)
        return result
    except:
        return seq


class count_big(GenericFunction):
    r"""The MSSQL count_big aggregate function.  With no arguments,
    emits COUNT \*.

    E.g.::

        from sqlalchemy import func
        from sqlalchemy import select
        from sqlalchemy import table, column

        my_table = table('some_table', column('id'))

        stmt = select(func.count_big()).select_from(my_table)

    Executing ``stmt`` would emit::

        SELECT count_big(*) AS count_1
        FROM some_table


    """
    type = sqltypes.Integer
    inherit_cache = True

    def __init__(self, expression=None, **kwargs):
        if expression is None:
            expression = literal_column("*")
        super(count_big, self).__init__(expression, **kwargs)
