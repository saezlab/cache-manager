from __future__ import annotations

from typing import Any, Iterable, Mapping

import hashlib

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
