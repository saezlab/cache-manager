from __future__ import annotations

from typing import Any, Mapping, Iterable
import re
import hashlib
import datetime
import collections

from pypath_common import _misc
import dateutil

__all__ = [
    'hash',
    'list_like',
    'parse_attr',
    'parse_attr_search',
    'parse_time',
    'serialize',
]

def list_like(value: Any) -> bool:
    """
    Checks whether a give value is list-like (e.g. `list`, `tuple`, etc.).

    Args:
        value:
            Any object instance or variable to ascertain.

    Returns:
        `True` if the value is iterable and not `str`, `False` otherwise.

    Examples:
        >>> list_like(123)
        False
        >>> list_like('abc')
        False
        >>> list_like([1, 2, 3])
        True
        >>> list_like((1, 2, 3))
        True
    """

    return (
        not isinstance(value, str) and
        isinstance(value, Iterable)
    )


def serialize(value: Any) -> str:
    """
    Converts a (collection of) variable(s) into a single string. NOTE: order of
    elements may not be kept.

    Args:
        value:
            Any object instance or variable to serialize as a string.

    Returns:
        The resulting serialized string of the value provided.

    Examples:
        >>> serialize({'a': 1, 'b': 2})
        '[a=1,b=2]'
        >>> serialize([{'a': [1, 2, 3, 4], 'b': {0, 1}}, 99, 'c'])
        '[99,[a=[1,2,3,4],b=[0,1]],c]'
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
    """
    Creates a unique hash for a given value. The value provided can be any type,
    which will be serialized (see `utils.serialize()`) from which the hash is
    generated using the MD5 hash function.

    Args:
        value:
            Any object instance or variable to generate a hash from.

    Returns:
        The unique hash for the provided value.

    Examples:
        >>> hash('abc')
        '900150983cd24fb0d6963f7d28e17f72'
        >>> hash([1, 2, 'a', 'b', {0, 1}])
        '94b41910362699b1f36754f78f1c90b2'
    """

    value = serialize(value).encode()

    return hashlib.md5(value).hexdigest()


def parse_time(value: str | datetime.datetime | None = None) -> str:
    """
    Formats a given date and time value as a string. If none is given, takes the
    current data and time as default.

    Args:
        value:
            String or `datetime.datetime` instance defining the time to format
            and convert to `str`. Optional, defaults to `None`

    Returns:
        Formatted date and time string as 'YYYY-MM-DD hh:mm:ss'

    Examples:
        >>> parse_time('21/12/20 12:31')
        '2020-12-21 12:31:00'
        >>> parse_time() # Will return current time
        '2024-08-09 13:08:28'
    """

    if isinstance(value, str):

        value = dateutil.parser.parse(value)

    elif value is None:

        value = datetime.datetime.now()

    return value.strftime('%Y-%m-%d %H:%M:%S')


def parse_attr_search(dct) -> dict:
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

    Returns:
        Returns a dictionary where the keys are the data type of the attributes
        and the values are a list of the different attributes to search on that
        attribute type table.
    """

    regex = re.compile(r'(.*[^<>=])([=<>]*)')

    result = collections.defaultdict(list)

    default_operator = '='

    for k, v in dct.items():
        if isinstance(v, list):
            values = [parse_attr(x) for x in v]

        else:
            values = [parse_attr(v)]

        name, operator = regex.match(k).groups()

        atype = {_atype for op, val, _atype in values}

        if len(atype) > 1:
            raise ValueError(f'Search values on attribute {name} have \
                             heterogeneous types')

        else:
            atype = _misc.first(atype)

        values_str = ' OR '.join(
            f'value {op or operator or default_operator} {val}'
            for op, val, _atype in values
        )

        result[atype].append(f'name = "{name}" AND ({values_str})')

    return result


def parse_attr(value):
    """
    Parse only one attribute.
    """

    atype = 'varchar'
    operator = None

    if isinstance(value, tuple):
        operator, value = value

    if isinstance(value, str):

        if value.lower().startswith('date:'):

            value = dateutil.parser.parse(value[5:])

        elif _misc.is_int(value):
            value = _misc.to_int(value)

        elif _misc.is_float(value):
            value = _misc.to_float(value)

    if isinstance(value, datetime.datetime):
        atype = 'datetime'
        value = f'"{parse_time(value)}"'

    elif isinstance(value, int):
        atype = 'int'
        value = str(value)

    elif isinstance(value, float):
        atype = 'float'
        value = str(value)

    else:
        value = f'"{value}"'

    return operator, value, atype
