import random
import re
import datetime


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


def sql_string(value):
    """Some Python types cast to String values that break SQL when cast to varchar.
    For example datetime includes 6-digit microseconds which breaks MSSQL.
    """
    if isinstance(value, (datetime.datetime)):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value
