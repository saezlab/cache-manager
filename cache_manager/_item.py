from __future__ import annotations

import os

import utils

__all__ = [
    'CacheItem',
]


class CacheItem:
    """
    Cache item class, stores a single cache item information.
    """

    def __init__(# TODO:add filename
            self,
            key,
            version: int = 1,
            status: int = 0,
            date: str = None,
            filename: str = None,
            ext: str | None = None,
            label: str | None = None,
            attrs: dict | None = None
    ):
        """
        Instantiates a new cache item.
        """

        self.key = key
        self.version = version
        self.status = status
        self.date = date
        self.filename = filename
        self.ext = ext
        self.label = label
        self.attrs = attrs or {}
        self._setup()

    @classmethod
    def new(
        cls,
        uri,
        params,
        version: int = 0,
        status: int = 0,
        date: str = None,
        filename: str = None,
        ext: str | None = None,
        label: str | None = None,
        attrs: dict | None = None,
    ):
        """
        Creates a new item.
        """

        key = cls.serialize(uri, params)
        args = {
            k: v for k, v in locals().items()
            if k not in ['uri', 'params', 'cls']
        }

        return cls(key, **args)

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
    
    def _setup(self):
        """
        Setting default values
        """

        self.filename = self.filename or os.path.basename(self.uri)
        self.ext = os.path.splitext(self.filename)[-1]
        self.date = self.date or utils.parse_time()