import logging
import random
import re
import urllib.request
from pathlib import Path

from sqlalchemy import literal_column
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.functions import GenericFunction


logger = logging.getLogger(__name__)


def ensure_gcs_shaded_jar(url: str) -> str:
    """Download a JAR from *url* to ~/.cache/dumpty/ if not already present.

    Returns the absolute path to the cached JAR.
    """
    jar_name = url.rsplit("/", 1)[-1]
    cache_dir = Path.home() / ".cache" / "dumpty"
    cache_dir.mkdir(parents=True, exist_ok=True)
    jar_path = cache_dir / jar_name
    if not jar_path.exists():
        logger.info("Downloading %s ...", url)
        urllib.request.urlretrieve(url, jar_path)  # noqa: S310
        logger.info("Saved to %s", jar_path)
    return str(jar_path)


def normalize_str(x: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", x).lower()


def filter_shuffle(seq: list) -> list:
    """
    Filter for Jinja to shuffle a list
    """
    try:
        result = list(seq)
        random.shuffle(result)
        return result
    except Exception:
        return seq


class CountBig(GenericFunction):
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

    name = "count_big"
    type = sqltypes.Integer()
    inherit_cache = True

    def __init__(self, expression=None, **kwargs):
        if expression is None:
            expression = literal_column("*")
        super().__init__(expression, **kwargs)
