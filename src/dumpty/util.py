from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, collect_list, col, struct
import re
import random


def normalize_str(x: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", x).lower()


def normalize_df(df: DataFrame) -> DataFrame:
    return df.select([col(x).alias(normalize_str(x)) for x in df.columns])


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
