from __future__ import annotations

__all__ = [
    'CacheItem',
]

from typing import IO, Type
import os

from pypath_common import _misc

from cache_manager import _open
from cache_manager._status import Status
import cache_manager
import cache_manager.utils as _utils


class CacheItem:
    """
    Cache item class, stores a single cache item information.

    NOTE: Alternative creation function can be found in the class method `new`.
    The `__init__` method creates an instance based on the `key` attribute
    (generally for internal use), while the `new` method takes a URI and is more
    intended for user interface.

    Args:
        key:
            Unique key name for the item. The creation method
            `CacheItem.new` provides it automatically as an alphanumeric
            string (see `CacheItem.serialize` for details).
        version:
            Version number of the item. Optional, defaults to `1`.
        status:
            Status of the entry as integer (see `_status.Status` for more
            info). Optional, defaults to `1`.
        date:
            Date of the entry, if none is provided, takes the current time.
            Optional, defaults to `None`.
        filename:
            Name of the file associated to the item. Optional, defaults to
            `None`.
        ext:
            Extension of the file associated to the item. Optional, defaults
            to `None` (automatically extracted from the file name).
        label:
            Label for the item (e.g. type, group, category...). Optional,
            defaults to `None`
        attrs:
            Extra attributes associated to the item. Keys are the attribute
            names and values their content. These attributes will be stored
            in the attribute tables according to their data type
            automatically. Optional, defaults to `None`.
        _id:
            Internal ID number. Optional, defaults to `None`.
        last_read:
            Date of last reading of the entry. Optional, defaults to `None`.
        last_search:
            Date of last search for the item. Optional, defaults to `None`.
        read_count:
            Counter of reads for the item. Optional, defaults to `None`.
        search_count:
            Counter for the times the item has been searched. Optional,
            defaults to `None`.
        cache:
            The `Cache` instance where the item belongs. Optional, defaults
            to `None`.

    Attrs:
        key:
            Unique key name for the item.
        version:
            Current version number of the item.
        date:
            Creation date of the entry.
        filename:
            Name of the file associated to the item.
        ext:
            Extension of the file associated to the item.
        label:
            Item label (e.g. type, group, category...).
        attrs:
            Extra attributes associated to the item. Keys are the attribute
            names and values their content.
        last_read:
            Date of last reading of the entry.
        last_search:
            Date of last search for the item.
        read_count:
            Counter of reads for the item.
        search_count:
            Counter for the times the item has been searched.
        cache:
            The `Cache` instance where the item belongs.
        cache_fname:
            The file name of the associated cache database.
        path:
            Path to the file associated to the item.
        rstatus:
            Current status of the item instance.
        status:
            Status of the item as registered in the `Cache`.
        uri:
            Uniform Resource Identifier.
        version_id:
            Identifier as a combination of item key and version number.
    """

    def __init__(
            self,
            key: str,
            version: int = 1,
            status: int = 0,
            date: str = None,
            filename: str = None,
            ext: str | None = None,
            label: str | None = None,
            attrs: dict | None = None,
            _id: int | None = None,
            last_read: str = None,
            last_search: str = None,
            read_count: int | None = None,
            search_count: int | None = None,
            cache: Type[cache_manager.Cache] | None = None,
    ):

        self.key = key
        self.version = version
        self._status = status
        self.date = date
        self.filename = filename
        self.ext = ext
        self.label = label
        self.attrs = attrs or {}
        self._id = _id
        self.last_read = last_read
        self.last_search = last_search
        self.read_count = read_count
        self.search_count = search_count
        self.cache = cache
        self._setup()


    def __repr__(self):

        return (
            f'CacheItem[{self.uri or self.key} V:{self.version} '
            f'{Status(self.rstatus).name}]'
        )


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
        last_read: str = None,
        last_search: str = None,
        read_count: int = 0,
        search_count: int = 0,
        cache: None = None,
    ) -> CacheItem:
        """
        Creates a new item.

        Args:
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters passed to the `__init__` method in dict
                format where key-value pairs correspond to parameter-value
                respectively. Optional, defaults to `None`.
            version:
                The version nuber of the new item. Optional, defaults to `0`.
            status:
                Status of the new item. Optional, defaults to `0`.
            date:
                Date of the entry, if none is provided, takes the current time.
                Optional, defaults to `None`.
            filename:
                Name of the file associated to the item. Optional, defaults to
                `None`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None` (automatically extracted from the file name).
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`
            attrs:
                Extra attributes associated to the item. Keys are the attribute
                names and values their content. These attributes will be stored
                in the attribute tables according to their data type
                automatically. Optional, defaults to `None`.
            last_read:
                Date of last reading of the entry. Optional, defaults to `None`.
            last_search:
                Date of last search for the item. Optional, defaults to `None`.
            read_count:
                Counter of reads for the item. Optional, defaults to `0`.
            search_count:
                Counter for the times the item has been searched. Optional,
                defaults to `0`.
            cache:
                The `Cache` instance where the item belongs. Optional, defaults
                to `None`.

        Returns:
            Instance of the newly created `CacheItem`.
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
    def serialize(cls, params: dict | None = None) -> str:
        """
        Serializes the parameters dictionary and hashes the result to generate
        an identifier. See `_utils.hash` for more information.

        Args:
            params:
                The parameters dictionary too generate the hash from.

        Returns:
            The resulting hash from the serailized parameters.
        """

        params = params or {}

        return _utils.hash(params)


    @property
    def cache_fname(self) -> str:
        """
        Gives the file name as stored in the cache.

        Returns:
            Combination of `version_id` (key + version) and the file extension.
        """

        ext = f'.{self.ext}' or ''

        return f'{self.version_id}{ext}'


    @property
    def path(self) -> str:
        """
        Gives the path of the file.

        Returns:
            The path to the cache item file (generally the cache path + the
            `cache_fname`).
        """

        d = self.cache.dir if self.cache else ''

        return os.path.join(d, self.cache_fname)


    @property
    def rstatus(self) -> int:
        """
        Gives the current status for the cache item instance.

        Returns:
            The integer value defining the status.
        """

        return self._status


    @property
    def status(self) -> int:
        """
        Gives the current status for the cache item as stored in the cache (if
        any, otherwise returns the status as stored in the `CacheItem`).

        Returns:
            The integer value defining the status.
        """

        return getattr(self._from_main(), '_status', self._status)


    @status.setter
    def status(self, value: int):
        """
        Sets the status to a given value, updating it in the associated `Cache`
        instance if available.

        Args:
            value:
                The value defining the new status to be set.
        """

        if self.cache:

            self.cache.update_status(
                key=self.key,
                version=self.version,
                status=value,
            )

        self._status = value


    @property
    def uri(self) -> str | None:
        """
        Provides the Uniform Resource Identifier if available.

        Returns:
            The Uniform Resource Identifier or `None` if not defined.
        """

        return self.attrs.get('_uri', None)


    @property
    def version_id(self) -> str:
        """
        Gives the unique version identifier based on the key of the item and the
        version number.

        Returns:
            Version identifier as [key]-[version].
        """

        return f'{self.key}-{self.version}'


    def failed(self):
        """
        Sets the status to failed.
        """

        self.status = Status.FAILED.value


    def open(self, **kwargs) -> str | IO | dict[str, str | IO] | None:
        """
        Opens the file in reading mode.

        Args:
            **kwargs:
                Keyword arguments are passed directly to the `Opener` class.

        Returns:
            The resulting opened file content. The type of content will depend
            on the passed arguments. See the `Opener` documentation for more
            details.
        """

        if self.status == Status.READY.value:

            return self._open(**kwargs).get('result', None)


    def ready(self):
        """
        Sets the status to ready.
        """

        self.status = Status.READY.value


    def remove(self, disk: bool = False, keep_record: bool = True):
        """
        Removes the item from the cache database (if available).

        Args:
            disk:
                Whether to also remove the file from disk. Optional, defaults to
                `False`.
            keep_record:
                Whether to keep the record in the database (i.e. marks the item
                status as trashed without actually deleting the record).
                Optional, defaults to `True`.
        """

        if self.cache:

            self.cache.remove(
                key=self.key,
                version=self.version,
                disk=disk,
                keep_record=keep_record,
            )


    def _from_main(self) -> CacheItem | None:
        """
        Retrieves the cache item instance from the cache database instance if
        available.

        Returns:
            The `CacheItem` instance retrieved as in the database.
        """

        if self.cache:

            return self.cache.by_key(self.key, self.version)


    def _open(self, **kwargs) -> _open.Opener:
        """
        Generates the `Opener` instance that opens the item file.

        Args:
            **kwargs:
                Keyword arguments are passed directly to the `Opener` class.

        Returns:
            The resulting `Opener` instance with the opened file. The type of
            content will depend on the passed arguments. See the `Opener`
            documentation for more details.
        """

        self.cache._accessed(self._id)

        return _open.Opener(self.path, **kwargs)


    def _setup(self):
        """
        Sets up default values for a new instance of a cache item. This infers
        file name, extension and date in case these are not provided.
        """

        self.filename = (
            self.filename or
            os.path.basename(self.uri or '') or
            self.cache_fname
        )
        self.ext = self.ext or os.path.splitext(self.filename)[-1][1:] or None
        self.date = self.date or _utils.parse_time()
