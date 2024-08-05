from __future__ import annotations

from typing import Any, Mapping, Iterable
import re
import hashlib
import datetime

import dateutil

from pypath_common import _misc

__all__ = [
    'hash',
    'list_like',
    'parse_attr_search',
    'parse_time',
    'serialize',
]

def list_like(value: Any):

    return (
        not isinstance(value, str) and
        isinstance(value, Iterable)
    )


def serialize(value: Any):
    """
    Converts collection of variables into a single string
    """

    if list_like(value):

        if isinstance(value, Mapping):

            value = [
                f'{serialize(k)}={serialize(v)}'
                for k, v in value.items()
            ]

        return '[%s]' % ','.join(sorted(map(serialize, value)))

    else:

        return str(value)


def hash(value: Any) -> str:

    value = serialize(value).encode()

    return hashlib.md5(value).hexdigest()


def parse_time(value: str | datetime.datetime | None = None) -> str:
    """
    Formats a date and time value.
    """

    if isinstance(value, str):

        value = dateutil.parser.parse(value)

    elif value is None:

        value = datetime.datetime.now()

    return value.strftime('%Y-%m-%d %H:%M:%S')

def parse_attr_search(dct): # TODO: WIP
    """
    Parse attribute search definition.

    Args:
        dct:
            Search by attributes. A dict of attribute names and values.
            Operators can be included at the end of the names or in front
            of the values, forming a tuple of length 2 in the latter case.
            Multiple values can be provided as lists. By default the
            attribute search parameters are joined by AND, this can be
            overridden by including `"__and": False` in `attrs`. The types
            of the attributes will be inferred from the values, except if
            the values provided as their correct type, such as numeric
            types or `datetime`. Strings will be converted to dates only if
            prefixed with `"DATE:"`.
    """
    regex = re.compile(r'(.*[^<>=])([=<>]*)')

    result = []

    for k, v in dct.items():
        if type(v) is tuple or type(v) is list:
            pass

        name, operator = regex.match(k).groups()

        if not operator:
            pass

def parse_attr(value):
    """
    Parse only one attribute.
    """

    atype = "varchar"
    operator = None

    if isinstance(value, tuple):
        operator, value = value

    if isinstance(value, str):

        if value.lower().startswith("date:"):
            value = datetime.datetime(value)

        elif _misc.is_int(value):
            value = _misc.to_int(value)

        elif _misc.is_float(value):
            value = _misc.to_float(value)

    if isinstance(value, datetime.datetime):
        atype = "datetime"
        value = parse_time(value)
