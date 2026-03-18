import logging
import os
import random
import re
import shutil
import tempfile
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

    # If a cached JAR exists and is non-empty, reuse it.
    if jar_path.exists():
        try:
            if jar_path.stat().st_size > 0:
                return str(jar_path)
            logger.warning("Cached JAR at %s is empty; re-downloading.", jar_path)
        except OSError:
            logger.warning("Could not stat cached JAR at %s; re-downloading.", jar_path)

    logger.info("Downloading %s ...", url)
    tmp_file = None
    try:
        # Create a temporary file in the same directory for an atomic move.
        with tempfile.NamedTemporaryFile(delete=False, dir=cache_dir) as tmp:
            tmp_file = Path(tmp.name)
            with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310
                shutil.copyfileobj(response, tmp)

        # Basic validation: ensure the downloaded file is non-empty.
        if tmp_file.stat().st_size <= 0:
            raise OSError(f"Downloaded JAR from {url} is empty.")

        # Atomically move the completed download into place.
        os.replace(tmp_file, jar_path)
        logger.info("Saved to %s", jar_path)
    except Exception:
        # Clean up temporary file on failure.
        if tmp_file is not None:
            try:
                tmp_file.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove temporary file %s", tmp_file, exc_info=True)
        raise

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
