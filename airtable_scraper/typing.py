from datetime import datetime, timedelta
from typing import Any

from dateutil.parser import parse
from pytz import timezone, utc


def join_list_like(lst: list | set | tuple | Any, sep: str = "") -> str:
    """Join list that contains values of None datatype.

    Args:
        lst (list | Any): List or list like object
        sep (str, optional): Separator string. Defaults to "".

    Returns:
        str: _description_
    """
    if not lst:
        return ""

    return sep.join(str(v) if v is not None else "" for v in lst)


def cast_to_str(value: int | float | list | set | tuple | dict | None, **kwargs) -> str:
    """Cast any python native datatype to string. If value is list, set, or tuple, function will return joined string with your specified separator.

    Args:
        value (int | float | list | set | tuple | dict | None): Any value that is a native python datatype, including strings that is datetime

    Kwargs:
        sep (str): Separator for string join. Keyword argument for join_listlike function
        tz (str): Timezone for converting time string to different timezone

    Returns:
        str: Casted string or joined string
    """
    if value is None:
        return ""

    if isinstance(value, str):
        if is_date(value):
            dt_utc = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
            dt_utc = dt_utc.replace(tzinfo=utc)
            dt = dt_utc.astimezone(timezone(kwargs["tz"]))

            if kwargs["date_only"]:
                dt = dt + timedelta(days=1)
                return dt.strftime("%B %d, %Y")

            return dt.strftime("%m/%d/%Y %I:%M%p").lower()

    if isinstance(value, list | tuple | set):
        return join_list_like(value, kwargs["sep"])

    if isinstance(value, dict):
        return join_list_like(value.values(), kwargs["sep"])

    return str(value)


def is_date(string: str, fuzzy: bool = False):
    """Check if string can be interpreted as datetime.

    Args:
        string (str): String
        fuzzy (bool, optional): Ignore unknown tokens if True. Defaults to False.

    Returns:
        bool: Boolean
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def none_filter(x: str | int | float | bool) -> str:
    """More basic cast_to_str() function. Casts data to string if not None.

    Args:
        x (str | int | float | bool): Input data

    Returns:
        str: Casted string or empty string if None
    """
    return str(x) if x is not None else ""
