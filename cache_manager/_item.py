from __future__ import annotations

import os

from pypath_common import _misc

import cache_manager.utils as _utils

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
            version: int = 1,
            status: int = 0,
            date: str = None,
            filename: str = None,
            ext: str | None = None,
            label: str | None = None,
            attrs: dict | None = None,
            _id: int | None = None,
            cache = None,
    ):
        """
        Instantiates a new cache item.
        """

        self.key = key
        self.version = version
        self._status = status
        self.date = date
        self.filename = filename
        self.ext = ext
        self.label = label
        self.attrs = attrs or {}
        self._id = _id
        self.cache = cache
        self._setup()

    @classmethod
    def new(
        cls,
        uri: str | None = None,
        params: dict | None = None,
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

        params = params or {}
        attrs = attrs or {}

        if uri:
            params['_uri'] = uri
            attrs['_uri'] = uri

        key = cls.serialize(params)
        args = {
            k: v for k, v in locals().items()
            if k not in ['uri', 'params', 'cls']
        }

        return cls(**args)

    @classmethod
    def serialize(cls, params: dict | None = None):
        """
        Serializes to generate an identifier.
        """

        params = params or {}

        return _utils.hash(_utils.serialize(params))

    def path(self, version: int | None = None):
        """
        Defines the path of the file.
        """

        version = self.default_version if version is None else version

        return f'{self.key}-{version}.{self.ext}'

    @property
    def uri(self):

        return self.attrs.get('_uri', None)

    def _setup(self):
        """
        Setting default values
        """
        # TODO: Fix URI/filename
        self.filename = self.filename# or os.path.basename(self.params['_uri'])
        #self.ext = os.path.splitext(self.filename)[-1]
        self.date = self.date or _utils.parse_time()


    def _from_main(self) -> CacheItem | None:

        if self.cache:

            return self.cache.by_key(self.key, self.version)


    @property
    def status(self):

        return getattr(self._from_main(), '_status', self._status)

    @status.setter
    def status(self, value: int):

        if self.cache:

            self.cache.update_status(
                key = self.key,
                version = self.version,
                status = value,
            )

        self._status = value
