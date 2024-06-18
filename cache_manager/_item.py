from __future__ import annotations

import os

from pypath_common import _misc

import cache_manager.utils as _utils
from cache_manager._status import status as _status

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
        cache: None = None,
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

    @property
    def path(self):
        """
        Defines the path of the file.
        """

        d = self.cache.dir if self.cache else ''
        ext = f'.{self.ext}' or ''

        return os.path.join(d, f'{self.key}-{self.version}{ext}')


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


    def ready(self):
        """
        Sets the status to ready.
        """

        self.status = _status.READY.value


    def failed(self):
        """
        Sets the status to failed.
        """

        self.status = _status.FAILED.value


    def remove(self):
        """
        Remove the item from the database.
        """

        if self.cache:

            self.cache.remove(key = self.key, version = self.version)
