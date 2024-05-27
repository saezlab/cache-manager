from __future__ import annotations

import utils

__all__ = [
    'CacheItem',
]


class CacheItem:
    """
    Cache item class, stores a single cache item information.
    """

    def __init__(
            self,
            key,
            version: int,
            status: int,
            date: str,
            ext: str | None = None,
            label: str | None = None,
            attrs: dict | None = None,
    ):
        """
        Instantiates a new cache item.
        """

        self.key = key
        self.version = version
        self.status = status
        self.date = date
        self.ext = ext
        self.label = label
        self.attrs = attrs or {}

    @classmethod
    def new(cls, uri, attrs):
        """
        Creates a new item.
        """

        key = cls.serialize(uri, attrs)

        return cls(key)

    @classmethod
    def serialize(cls, uri, attrs: dict | None = None):
        """
        Serializes to generate an identifier.
        """

        attrs = attrs or {}
        attrs['uri'] = uri

        return utils.serialize(attrs)

    def path(self, version: int | None = None):
        """
        Defines the path of the file.
        """

        version = self.default_version if version is None else version

        return f'{self.key}-{version}.{self.ext}'
