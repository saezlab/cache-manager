from __future__ import annotations

import enum

__all__ = ['status']


class status(enum.Enum):

    UNINITIALIZED = 0
    WRITE = 1
    FAILED = 2
    READY = 3

    @classmethod
    def from_str(cls, name: str) -> status:

        return cls.__dict__[name.upper()]
