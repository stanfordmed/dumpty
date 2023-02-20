import random
import re


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
