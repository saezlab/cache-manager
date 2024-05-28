from __future__ import annotations

from typing import Any, Mapping, Iterable
import hashlib
import datetime

import dateutil

__all__ = [
    'hash',
    'list_like',
    'parse_time',
    'serialize',
]

def list_like(value: Any):

    return (
        not isinstance(value, str) and
        isinstance(value, Iterable)
    )


def serialize(value: Any):

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


def parse_time(value: str | datetime.datetime) -> str:
    """
    Formats a date and time value.
    """

    if isinstance(value, str):

        value = dateutil.parser.parse(value)

    elif value is None:

        value = datetime.datetime.now()

    return datetime.strftime(value, '%Y-%m-%d %H:%M:%S')
