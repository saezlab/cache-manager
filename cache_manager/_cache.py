from __future__ import annotations

__all__ = [
    'ATTR_TYPES',
    'Cache',
    'TYPES',
]

from typing import Any
import os
import re
import json
import shutil
import sqlite3
import datetime
import functools as ft
import collections

from pypath_common import _misc
import platformdirs

from cache_manager._item import CacheItem
from cache_manager._status import Status
from cache_manager._session import _log
import cache_manager.utils as _utils
from . import _data
from ._lock import Lock

ATTR_TYPES = ['varchar', 'int', 'datetime', 'float', 'text']

TYPES = {
    'str': 'VARCHAR',
    'int': 'INT',
    'float': 'FLOAT',
    'datetime': 'DATETIME',
    'list': 'TEXT',
    'dict': 'TEXT',
    'set': 'TEXT',
    'tuple': 'TEXT',
}


class Cache:
    """
    Cache manager class, stores and manages the information in the registry
    database as well as the files in the cache directory.

    Args:
        path:
            Explicit path to set the cache in. Overrides the `pkg` keyword
            argument. Optional, defaults to `None`.
        pkg:
            Package/module name the cache is used on. This sets the cache
            directory in a folder located in the OS default cache directory.

    Attrs:
        con:
            Current connection to the SQL database, an instance of
            `sqlite3.Connection`.
        cur:
            Current cursor of the SQL database, an instance of `sqlite3.Cursor`.
        path:
            Path to the current cache registry.
        dir:
            The directory of the cache.
        free_space:
            Amount of free space available in the cache (in bytes).
    """

    def __init__(self, path: str | None = None, pkg: str | None = None):

        self.con, self.cur = None, None
        self._fields = {}
        self._set_path(path=path, pkg=pkg)
        self._ensure_sqlite()


    def __del__(self):

        if hasattr(self, 'con'):

            _log(f'Closing SQLite database path: {self.path}')
            self.con.close()


    def __len__(self):

        self._ensure_sqlite()

        return self.cur.execute('SELECT COUNT(*) FROM main').fetchone()[0]


    @property
    def free_space(self) -> int:
        """
        Calculates the available free space in the cache directory.

        Returns:
            The available space in bytes.
        """

        total, used, free = shutil.disk_usage(self.dir)

        return free


    def autoclean(self):
        """
        Keeps only ready/in writing items and for each item the best version and
        deletes anything else in the cache registry.
        """

        _log('Auto cleaning cache.')
        items = collections.defaultdict(set)
        best = dict()

        for it in self.contents().values():

            if (item := it['item']):

                items[item.key].add(item)

        best = {
            key: _misc.first([
                it for it in sorted(its, key=lambda x: x.version)[::-1]
                if it._status in {Status.READY.value, Status.WRITE.value}
            ])
            for key, its in items.items()
        }

        to_remove = [
            it for k, v in items.items()
            for it in v - _misc.to_set(best.get(k, []))
        ]

        _log(f'Deleting {len(to_remove)} records.')

        self._delete_records(to_remove)
        self.clean_disk()
        _log('Auto clean complete.')


    def best(
            self,
            uri: str,
            params: dict | None = None,
            status: int | set[int] | None = Status.READY.value,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ) -> CacheItem | None:
        """
        Searches for the best (latest) version of an item in the cache registry.

        Args:
            uri:
                Uniform Resource Identifier.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            status:
                Integer (or set of) defining the valid status of the item to be
                searched. Optional, defaults to `3` (READY status).
            newer_than:
                Date the times are required to be newer than. Optional, defaults
                to `None`.
            older_than:
                Date the times are required to be older than. Optional, defaults
                to `None`.

        Returns:
            The `CacheItem` instance corresponding to the latest version of it.

        Example:
            >>> cache = cm.Cache('./')
            >>> cache.create('foo')
            CacheItem[foo V:1 UNINITIALIZED]
            >>> cache.create('foo')
            CacheItem[foo V:2 UNINITIALIZED]
            >>> cache.best('foo', status=0)
            CacheItem[foo V:2 UNINITIALIZED]
        """

        status = _misc.to_set(status)

        items = self.search(
            uri=uri,
            params=params,
            status=status,
            newer_than=newer_than,
            older_than=older_than,
        )
        # TODO: Consider also date
        items = sorted(items, key=lambda it: it.version)

        if items:

            _log(f'Best matching version: {items[-1].version}')

            return items[-1]

        _log('No version found matching criteria')


    def best_or_new(
        self,
        uri: str,
        params: dict | None = None,
        status: int | set[int] | None = Status.READY.value,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
        attrs: dict | None = None,
        ext: str | None = None,
        label: str | None = None,
        new_status: int = Status.WRITE.value,
        filename: str | None = None,
    ) -> CacheItem:
        """
        Searches for the best version of an item (i.e. last version). If such
        item could not be found, it creates a new one.

        Args:
            uri:
                Uniform Resource Identifier.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            status:
                Integer (or set of) defining the valid status of the item to be
                searched. Optional, defaults to `3` (READY status).
            newer_than:
                Date the times are required to be newer than. Optional, defaults
                to `None`.
            older_than:
                Date the times are required to be older than. Optional, defaults
                to `None`.
            attrs:
                Attributes of the item to be searched or created as dictionary
                of key-value pairs corresponding to the name and value of the
                attributes. Optional, defaults to `None`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None`. Currently not implemented.
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`.
            new_status:
                Integer defining the new status to be set in the case a new item
                is created. Optional, defaults to `1` (WRITE status).
            filename:
                Name of the file associated to the item. Optional, defaults to
                `None`.

        Returns:
            The `CacheItem` instance of the best or new item according to the
            provided attributes.

        Examples:
            >>> cache = cm.Cache('./')
            >>> cache.create('foo')
            CacheItem[foo V:1 UNINITIALIZED]
            >>> cache.create('foo')
            CacheItem[foo V:2 UNINITIALIZED]
            >>> cache.best_or_new('foo', status=0)
            CacheItem[foo V:2 UNINITIALIZED]
            >>> cache.best_or_new('bar')
            CacheItem[bar V:1 WRITE]
        """

        args = locals()
        args.pop('self')
        args['status'] = args.pop('new_status')
        args.pop('newer_than')
        args.pop('older_than')

        with Lock(self.con):

            item = self.best(
                uri=uri,
                params=params,
                status=status,
                newer_than=newer_than,
                older_than=older_than,
            )

            if not item:

                item = self.create(**args)

        return item


    def by_attrs(self, attrs: dict) -> set[int]:
        """
        Searches entries in the registry based on their attributes (stored in
        the differen type-based attribute tables).

        Args:
            attrs:
                Attributes and corresponding values of the items to search for.
                By default, the different attributes in the search must be
                satisfied. This is, items that fulfill all the attribute-value
                pairs, will be included in the search result. In case one wants
                the results of the search to just fulfill at least one term, it
                must include the following key-value pair in the argument:
                `'__and': False`. See example below.

        Returns:
            Set of keys corresponding to the elements in the registry with the
            searched attributes.

        Examples:
            >>> cache = cm.Cache('./')
            >>> cache.create('foo1', attrs={'bar': 1, 'baz': 2})
            CacheItem[foo1 V:1 UNINITIALIZED]
            >>> cache.create('foo2', attrs={'bar': 1, 'baz': 5})
            CacheItem[foo2 V:1 UNINITIALIZED]
            >>> cache.by_attrs({'bar': 1, 'baz': 5})
            {2}
            >>> cache.by_attrs({'bar': 1, 'baz': 5, '__and': False})
            {1, 2}
        """

        _log(f'Searching by attributes: {attrs}')
        result = []

        op = set.intersection if attrs.pop('__and', True) else set.union
        attrs = _utils.parse_attr_search(attrs)

        for atype, queries in attrs.items():

            for query in queries:

                self._execute(f'SELECT id FROM attr_{atype} WHERE {query}')
                aux = self.cur.fetchall()
                result.append({item[0] for item in aux})

        return op(*result) if result else set()


    def by_key(self, key: str, version: int) -> CacheItem:
        """
        Searches a single item by its key and version number.

        Args:
            key:
                The key of the item to be fetched.
            version:
                The specific version of the item to be retrieved.

        Returns:
            The `CacheItem` instance of the item searched (if any).

        Example:
            >>> cache = cm.Cache('./')
            >>> it = cache.create('foo')
            >>> it.key
            '31d0e534960b07c0bde745c17b05eaba'
            >>> cache.by_key('31d0e534960b07c0bde745c17b05eaba', 1)
            CacheItem[foo V:1 UNINITIALIZED]
        """

        _log(f'Looking up key: {key}')

        return _misc.first(self.search(key=key, version=version))


    def clean_db(self):
        """
        Removes records from the database registry that do not have the
        corresponding file on the cache disk directory.
        """

        _log('Cleaning cache database: removing records without file on disk.')

        items = {
            item
            for it in self.contents().values()
            if (item := it['item'])
            and not os.path.exists(it['item'].path)
        }
        _log(f'Deleting {len(items)} records.')

        self._delete_records(items)
        _log('Cleaning cache database complete.')


    def clean_disk(self):
        """
        Deletes files from the disk cache directory if they don't have any
        record in the database registry.
        """

        _log('Cleaning disk: removing items without DB record.')

        fnames = {
            os.path.join(self.dir, fname)
            for item in self.contents().values()
            if (fname := item['disk_fname'])
            and not item.get('status', False)
        }

        _log(f'Deleting {len(fnames)} files.')

        for file in fnames:

            _log(f'Deleting from disk: `{file}`.')
            os.remove(file)

        _log('Cleaning disk complete.')


    def contents(self) -> dict[str, dict[str, int | str | CacheItem]]:
        """
        Generates a collection of all the items in the database registry and
        files in the cache directory on the disk.

        Returns:
            Dictionary where keys correspond to each item's `version_id` and
            values to dictionary with some of the item's attributes, namely:
            `status` (current status of the item as integer), `fname` (file name
            as stored in the cache database), `last_read` (date where the item
            was last accessed), `read_count` (number of times the item has been
            accessed), `item` (the instance of the `CacheItem` itself),
            `disk_fname` (file name as stored in the cache directory on the
            disk).

        Example:
            >>> cache = cm.Cache('./')
            >>> cache.create('foo')
            CacheItem[foo V:1 UNINITIALIZED]
            >>> cache.contents()
            {'31d0e534960b07c0bde745c17b05eaba-1': {'status': 0, 'fname': '31d0\
            e534960b07c0bde745c17b05eaba-1', 'last_read': None, 'read_count': 0\
            , 'item': CacheItem[foo V:1 UNINITIALIZED], 'disk_fname': None}}
        """

        disk = {
            m.group(): fname
            for fname in os.listdir(self.dir)
            if (m := re.search(r'[\dabcdef]{32}-\d+', fname))
        }

        db = {
            it.version_id: {
                'status': it._status,
                'fname': it.cache_fname,
                'last_read': it.last_read,
                'read_count': it.read_count,
                'item': it,
            }
            for it in self.search(include_removed = True)
        }

        return {
            vid: dict(**db.get(vid, {}), disk_fname = disk.get(vid, None))
            for vid in set(disk.keys()) | set(db.keys())
        }


    def create(
            self,
            uri: str,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int = Status.UNINITIALIZED.value,
            ext: str | None = None,
            label: str | None = None,
            filename: str | None = None,
    ) -> CacheItem:
        """
        Creates a new entry in the registry.

        Args:
            uri:
                Uniform Resource Identifier.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            attrs:
                Extra attributes associated to the item. Keys are the attribute
                names and values their content. These attributes will be stored
                in the attribute tables according to their data type
                automatically. Optional, defaults to `None`.
            status:
                Status of the new item. Optional, defaults to `0`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None` (automatically extracted from the file name).
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`.
            filename:
                Name of the file associated to the item. Optional, defaults to
                `None` (automatically set).

        Returns:
            The newly created `CacheItem` instance.

        Example:
            >>> cache = cm.Cache('./')
            >>> cache.create('foo')
            CacheItem[31d0e534960b07c0bde745c17b05eaba V:1 UNINITIALIZED]

        """

        self._ensure_sqlite()

        _log(f'CREATE {uri}')
        args = locals()
        args.pop('self')
        param_str = _utils.serialize(args)

        _log(f'Creating new version for item {param_str}')

        with Lock(self.con):

            _log(f'Looking up existing versions of item `{uri}`')
            items = self.search(
                uri=uri,
                params=params,
            )

            last_version = max((i.version for i in items), default = 0)

            if last_version == 0:

                _log('No existing version found.')

            else:

                _log(f'Latest version: `{last_version}`')

            new = CacheItem.new(
                uri,
                params,
                attrs=attrs,
                version=last_version + 1,
                date=_utils.parse_time(),
                status=status,
                ext=ext,
                label=label,
                cache=self,
            )

            _log(f'Next version: {new.key}-{new.version}')

            self._execute(f'''
                INSERT INTO
                main (
                    item_id,
                    version_id,
                    version,
                    status,
                    file_name,
                    label,
                    date,
                    ext,
                    last_read,
                    last_search,
                    read_count,
                    search_count
                )
                VALUES (
                    {self._quotes(new.key)},
                    "{new.key}-{new.version}",
                    {new.version},
                    {new._status},
                    {self._quotes(new.filename)},
                    {self._quotes(new.label)},
                    {self._quotes(new.date)},
                    {self._quotes(new.ext)},
                    NULL,
                    {self._quotes(new.date)},
                    0,
                    0
                )
            ''')

            q = f'SELECT id FROM main WHERE version_id = "{new.version_id}"'
            self._execute(q)
            key = self.cur.fetchone()[0]
            new._id = key

            for actual_typ in ATTR_TYPES:

                _log(f'Creating attributes in attr_{actual_typ}')

                # BEWARE
                useattrs = [
                    (keyvar, group if isinstance(vals, dict) else 'NULL', k, v)
                    for keyvar, d in enumerate(('attrs', 'params'))
                    for group, vals in getattr(new, d).items()
                    for k, v in (
                        vals
                        if isinstance(vals, dict) else
                        {group: vals}
                    ).items()
                    if self._sqlite_type(v) == actual_typ.upper()
                ]

                if not useattrs:

                    continue

                main_fields = self._table_fields()


                values = ', '.join(
                    f'({key}, {self._quotes(group)}, {keyvar}, '
                    f'"{k}", {self._quotes(v, actual_typ)})'
                    for keyvar, group, k, v in useattrs
                )

                q = (
                    f'INSERT INTO attr_{actual_typ} '
                    '( id, namespace, keyvar, name, value ) '
                    f'VALUES {values}'
                )

                self._execute(q)

            _log(f'Successfully created: {new.version_id}')

        _log('END CREATE')

        return new


    def does_it_fit(self, size: int) -> bool:
        """
        Checks whether a given size is lower than the current available space.

        Args:
            size:
                Integer corresponding to the size to be checked (in bytes).

        Returns:
            Whether the requested space is available.
        """

        return size <= self.free_space


    # TODO: Should method below include a call to `does_it_fit`?
    def move_in(
        self,
        path: str,
        uri: str | None = None,
        params: dict | None = None,
        attrs: dict | None = None,
        status: int = Status.WRITE.value,
        ext: str | None = None,
        label: str | None = None,
        filename: str | None = None,
    ) -> CacheItem:
        """
        Copies a file into the cache directory and creates the corresponding
        cache item registry.

        Args:
            path:
                Current/original path of the file that has to be moved into the
                cache.
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            attrs:
                Extra attributes associated to the item. Keys are the attribute
                names and values their content. These attributes will be stored
                in the attribute tables according to their data type
                automatically. Optional, defaults to `None`.
            status:
                Status of the new item. Optional, defaults to `1`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None` (automatically extracted from the file name).
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`.
            filename:
                Name of the file associated to the item. Optional, defaults to
                `None` (automatically set).

        Returns:
            The newly created `CacheItem` instance.
        """

        args = locals()
        args.pop('self')
        args.pop('path')

        uri = uri or os.path.basename(path)

        item = self.create(**args)
        _log(f'Copying `{path}` to `{item.path}`.')
        shutil.copy(path, item.path)

        return item


    def reload(self):
        """
        Reloads the cache_manager at the module level and reloads the current
        instance of `Cache`
        """

        modname = self.__class__.__module__
        mod = __import__(modname, fromlist=[modname.split('.')[0]])

        import importlib as imp

        imp.reload(mod)
        new = getattr(mod, self.__class__.__name__)
        setattr(self, '__class__', new)


    # FIXME: attrs, ext and label are not used
    # TODO: Make it more safer later (avoid to delete everything accidentally)
    def remove(
            self,
            uri: str | None = None,
            params: dict | None = None,
            version: int | set[int] | None = None,
            attrs: dict | None = None,
            status: int | None = None,
            ext: str | None = None,
            label: str | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            key: str | None = None,
            disk: bool = False,
            keep_record: bool = True,
    ) -> None:
        """
        Removes item(s) from the cache. The removal procedure will depend on the
        parameters `disk` and `keep_record`, see argument description below for
        specifics on their behavior.

        Args:
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            version:
                Integer defining the version of the item to update. Optional,
                defaults to `None`.
            attrs:
                Extra attributes associated to the item. Keys are the attribute
                names and values their content. Optional, defaults to `None`.
                Currently not implemented
            status:
                Integer defining the status of the item to update. Optional,
                defaults to `None`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None`. Currently not implemented.
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`. Currently not implemented.
            newer_than:
                Date the times are required to be newer than. Optional, defaults
                to `None`.
            older_than:
                Date the times are required to be older than. Optional, defaults
                to `None`.
            key:
                Unique key name for the item. Optional, defaults to `None`.
            disk:
                Whether to also remove the files associated to the entry(ies)
                from disk too. Optional, defaults to `False`.
            keep_record:
                Whether to keep the record of the entry in the registry (marks
                the entry status as trashed, status = -1). Otherwise the entry
                is permanently deleted. Optional, `True` by default.

        Example:
            >>> cache = cm.Cache('./')
            >>> cache.create('foo')
            CacheItem[foo V:1 UNINITIALIZED]
            >>> cache.remove(uri='foo')
            >>> cache.search(uri='foo')
            []
        """

        with Lock(self.con):

            items = self.search(
                uri=uri,
                params=params,
                status=status,
                version=version,
                newer_than=newer_than,
                older_than=older_than,
                key=key,
            )

            if not items:

                return

            where = ','.join(str(item._id) for item in items)
            where = f' WHERE id IN ({where})'
            new_status = Status.DELETED.value if disk else Status.TRASH.value

            q = f'UPDATE main SET status = {new_status} {where};'
            self._execute(q)

            if disk:

                self._delete_files(items)

            if not keep_record:

                self._delete_records(items)


    def search(
            self,
            uri: str | None = None,
            params: dict | None = None,
            status: int | set[int] | None = None,
            version: int | set[int] | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            ext: str | None = None,
            label: str | None = None,
            filename: str | None = None,
            key: str | None = None,
            attrs: dict | None = None,
            include_removed: bool = False,
    ) -> list[CacheItem]:
        """
        Looks up for items in the cache based on the passed parameter(s).

        Args:
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            status:
                Integer defining the status of the item to update. Optional,
                defaults to `None`.
            version:
                Integer defining the version of the item to update. Optional,
                defaults to `None`.
            newer_than:
                Date the times are required to be newer than. Optional, defaults
                to `None`.
            older_than:
                Date the times are required to be older than. Optional, defaults
                to `None`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None`.
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`.
            filename:
                Name of the file associated to the item. Optional, defaults to
                `None`.
            key:
                Unique key name for the item. Optional, defaults to `None`.
            attrs:
                Search by attributes. A dict of attribute names and values.
                Operators can be included at the end of the names or in front
                of the values, forming a tuple of length 2 in the latter case.
                Multiple values can be provided as lists. By default the
                attribute search parameters are joined by AND, this can be
                overridden by including `"__and": False` in `attrs`. The types
                of the attributes will be inferred from the values, except if
                the values provided as their correct type, such as numeric
                types or `datetime`. Strings will be converted to dates only if
                prefixed with `"DATE:"`. Optional, defaults to `None`.
            include_removed:
                Whether to include items marked for removal (i.e. trashed,
                status = -1) in the search.

        Returns:
            List of `CacheItem` instances of the items fulfilling the search.
            terms.

        Example:
            >>> cache = cm.Cache('./')
            >>> it = cache.create('foo')
            >>> cache.search(uri='foo)
            [CacheItem[foo V:1 UNINITIALIZED]]
        """

        _log('SEARCH')
        args = locals()
        args.pop('self')
        param_str = _utils.serialize(args)
        _log(f'Searching cache: {param_str}')
        attrs = args.pop('attrs') or {}
        ids = self.by_attrs(attrs)
        where = self._where(**args)

        if attrs:

            where += f' AND main.id IN ({",".join(str(i) for i in ids)})'

        keys = (
            list(self._table_fields().keys()) +
            ['namespace',  'keyvar', 'name', 'value']
        )

        results = {}

        with Lock(self.con):

            for actual_typ in ATTR_TYPES:

                fields = keys.copy()
                fields[0] = 'main.id'

                if actual_typ.upper() == 'TEXT':

                    fields[-1] = 'json(value)'

                fields = ', '.join(fields)

                q = (
                    f'SELECT {fields} FROM main '
                    f'LEFT JOIN attr_{actual_typ} attr ON main.id = attr.id '
                    f'{where}'
                )

                self._execute(q)

                _log(f'Fetching results from attr_{actual_typ}')

                for row in self.cur.fetchall():

                    row = dict(zip(keys, row))

                    if actual_typ.upper() == 'TEXT' and row['value']:

                        row['value'] = json.loads(row['value'])

                    verid = row['version_id']

                    if verid not in results:

                        _log(f'Found version: `{verid}`')

                        results[verid] = CacheItem(
                            key=row['item_id'],
                            version=row['version'],
                            status=row['status'],
                            date=row['date'],
                            filename=row['file_name'],
                            ext=row['ext'],
                            label=row['label'],
                            _id=row['id'],
                            last_read=row['last_read'],
                            last_search=row['last_search'],
                            read_count=row['read_count'],
                            search_count=row['search_count'],
                            cache=self,
                        )

                    if row['name']:

                        target = getattr(
                            results[verid],
                            'params' if row['keyvar'] else 'attrs',
                        )

                        if (
                            row['namespace'] is not None
                            and row['namespace'] not in target
                        ):

                            target[row['namespace']] = {}
                            target = target[row['namespace']]

                        target[row['name']] = row['value']

            if results:

                ids = ','.join(str(item._id) for item in results.values())
                update_q = (
                    'UPDATE main SET '
                    'last_search = DATETIME("now"), '
                    'search_count = search_count + 1 '
                    f'WHERE id IN ({ids});'
                )
                self._execute(update_q)

        _log(f'Retrieved {len(results)} results')
        _log('END SEARCH')

        return list(results.values())


    # FIXME: attrs, ext and label are not used
    def update(
            self,
            uri: str | None = None,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int | None = None,
            version: int | None = None,
            ext: str | None = None,
            label: str | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            key: str | None = None,
            update: dict | None = None,
    ):
        """
        Updates one or more items. All arguments except `update` are used to
        search for the items to be updated.

        Args:
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            attrs:
                Extra attributes associated to the item. Keys are the attribute
                names and values their content. Optional, defaults to `None`.
                Currently not implemented
            status:
                Integer defining the status of the item to update. Optional,
                defaults to `None`.
            version:
                Integer defining the version of the item to update. Optional,
                defaults to `None`.
            ext:
                Extension of the file associated to the item. Optional, defaults
                to `None`. Currently not implemented.
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`. Currently not implemented.
            newer_than:
                Date the times are required to be newer than. Optional, defaults
                to `None`.
            older_than:
                Date the times are required to be older than. Optional, defaults
                to `None`.
            key:
                Unique key name for the item. Optional, defaults to `None`.
            update:
                Dictionary containing the key-value pairs of fields/attributes
                and the new values respectively to be updated. Optional,
                defaults to `None`.

        Example:
            >>> cache = cm.Cache('./')
            >>> it = cache.create('foo', attrs={'bar': 123, 'baz': 456})
            >>> it.attrs
            {'bar': 123, 'baz': 456, '_uri': 'foo'}
            >>> cache.update(uri='foo', update={'bar': 0})
            >>> it = cache.search('foo')[0]
            >>> it.attrs
            {'_uri': 'foo', 'bar': 0, 'baz': 456}
        """

        with Lock(self.con):

            items = self.search(
                uri=uri,
                params=params,
                status=status,
                version=version,
                newer_than=newer_than,
                older_than=older_than,
                key=key,
            )

            update = update or {}
            main_fields = self._table_fields()
            main = ', '.join(
                f'{k} = {self._quotes(v, TYPES[type(v).__name__])}'
                for k, v in update.items() if k in main_fields
            )

            # Updating elements in main table
            ids = [it._id for it in items]
            _log(f'Updating {len(ids)} items')
            where = f' WHERE id IN ({", ".join(map(str, ids))})'

            if main:

                q = f'UPDATE main SET {main}{where};'
                self._execute(q)

            # Updating elements in attribute tables
            for actual_typ in ATTR_TYPES:

                _log(f'Updating attributes in attr_{actual_typ}')

                for k, v in update.items():

                    typ = type(v).__name__

                    if k not in main_fields and typ == actual_typ:

                        val = f'value = {self._quotes(v, TYPES[typ])}'
                        name_where = where + f' AND name = {self._quotes(k)}'
                        q = f'UPDATE attr_{actual_typ} SET {val} {name_where}'
                        self._execute(q)

            _log(f'Finished updating attributes')


    def update_status(
        self,
        uri: str | None = None,
        params: dict | None = None,
        version: int | None = -1,
        status: int = Status.READY.value,
        key: str | None = None,
    ):
        """
        Updates the status of a given entry(ies) in the registry. All arguments
        other than `status` are used to identify/search the entry(ies) to
        update.

        Args:
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            version:
                Version number of the item(s). Optional, defaults to `None`.
            status:
                Integer defining the new status to be set. Optional, defaults to
                `3` (READY status).
            key:
                Unique identifier for the item (alphanumeric hash).

        Example:
            >>> cache = cm.Cache('./')
            >>> it = cache.create('foo')
            >>> it.status
            0
            >>> cache.update_status(uri='foo')
            >>> it.status
            3
        """

        self.update(
            uri=uri,
            params=params,
            version=version,
            update={'status': status},
            key=key,
        )


    ready = ft.partialmethod(update_status, status=Status.READY.value)
    failed = ft.partialmethod(update_status, status=Status.FAILED.value)


    def _accessed(self, item_id: int):
        """
        Updates the 'last_read' and 'read_count' attributes of a given item to
        current date/time and +1 respectively.

        Args:
            item_id:
                Integer corresponding to the internal `CacheItem._id` attribute
                that has just been accessed.
        """

        q = (
            'UPDATE main SET '
            'last_read = DATETIME("now"), read_count = read_count + 1 '
            f'WHERE id = {item_id};'
        )
        self._execute(q)


    def _create_schema(self):
        """
        Initializes the SQL registry database and creates the main and attribute
        tables if not already existing.
        """

        self._ensure_sqlite()

        _log(f'Initializing database')

        fields = ', '.join(f'{k} {v}' for k, v in self._table_fields().items())

        _log(f'Ensuring main table exists')
        self._execute(f'''
            CREATE TABLE IF NOT EXISTS
            main (
                {fields}
            )
        ''')

        for typ in ATTR_TYPES:

            _log(f'Ensuring attr_{typ} table exists')
            self._execute(
                '''
                CREATE TABLE IF NOT EXISTS
                attr_{} (
                    id INT,
                    namespace VARCHAR,
                    keyvar INT,
                    name VARCHAR,
                    value {},
                    FOREIGN KEY(id) REFERENCES main(id)
                )
            '''.format(typ, typ.upper()),
            )


    def _delete_files(self, items: list[int, CacheItem]):
        """
        Permanently deletes the files from a given list of items in the cache
        from the disk.

        Args:
            items:
                List of items to be deleted, these can be either the `CacheItem`
                instances or an integer corresponding to the internal
                `CacheItem._id` attribute.
        """

        for item in items:

            if os.path.exists(item.path):

                _log(f'Deleting from disk: `{item.path}`.')
                os.remove(item.path)


    def _delete_records(self, items: list[int, CacheItem]):
        """
        Permanently deletes a given list of items from the cache registry.

        Args:
            items:
                List of items to be deleted, these can be either the `CacheItem`
                instances or an integer corresponding to the internal
                `CacheItem._id` attribute.
        """

        with Lock(self.con):

            where = ','.join(str(getattr(i, '_id', i)) for i in items)
            where = f' WHERE id IN ({where})'
            _log(f'_delete_records: {len(items)} IDs to be deleted.')
            n_before = len(self)

            for actual_typ in ATTR_TYPES:

                attr_table = f'attr_{actual_typ}'

                _log(f'Deleting attributes from {attr_table}')

                q = f'DELETE FROM {attr_table} {where}'

                self._execute(q)

            q = f'DELETE FROM main'
            q += where

            self._execute(q)

            _log(f'Deleted {n_before - len(self)} records.')


    def _ensure_sqlite(self):
        """
        Ensures the connection to the SQL database is open.
        """

        if self.con is None:

            self._open_sqlite()


    def _execute(self, query: str):
        """
        Executes a given SQL query in the database.

        Args:
            query:
                The SQL query string to execute in the database.
        """

        query = re.sub(r'\s+', ' ', query)
        _log(f'Executing query: {query}')
        self.cur.execute(query)
        self.con.commit()


    def _open_sqlite(self):
        """
        Opens the cache registry (SQL database) connection.
        """

        _log(f'Opening SQLite database: {self.path}')
        self.con = sqlite3.connect(self.path)
        self.cur = self.con.cursor()
        self._create_schema()


    def _set_path(self, path: str | None, pkg: str | None = None):
        """
        Sets the path for the cache. It can either be a explicitly defined path
        or can take a module/package name and set the path to the OS default
        cache directory and create a cache folder under the package name.

        Args:
            path:
                Explicit path to set the cache in. Overrides the `pkg` keyword
                argument. Optional, defaults to `None`.
            pkg:
                Package/module name the cache is used on. This sets the cache
                directory in a folder located in the OS default cache directory.

        Example:
        >>> cache = cm.Cache('.')
        >>> cache._set_path(path='./test_cache')
        >>> cache.dir
        './test_cache'
        >>> cache.path
        './test_cache/cache.sqlite'
        """

        if not path and not pkg:

            raise ValueError('Please provide a valid path or package name')

        path = path or platformdirs.user_cache_dir(pkg)

        if not os.path.exists(path):

            stem, ext = os.path.splitext(path)
            os.makedirs(stem if ext else path, exist_ok=True)

        if os.path.isdir(path):

            path = os.path.join(path, 'cache.sqlite')

        _log(f'Setting SQLite database path: {path}')
        self.path = path
        self.dir = os.path.dirname(self.path)


    def _table_fields(self, name: str = 'main') -> dict[str, str]:
        """
        Retrieves the available fields in the main table (i.e. column names).

        Args:
            name:
                Name of the table from which to retreive the field names.
                Optional, defaults to `'main'` (currently only option).

        Returns:
            Dictionary containing the field names as keys and values correspond
            to the SQL data types and column specifications.

        Example:
            >>> cache = Cache('./')
            >>> cache._table_fields()
            OrderedDict([('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'), \
                ('item_id', 'VARCHAR'), ('version_id', 'VARCHAR'), \
                ('version', 'INT'), ('status', 'INT'), \
                ('file_name', 'VARCHAR'), ('label', 'VARCHAR'), \
                ('date', 'DATETIME'), ('ext', 'VARCHAR'), \
                ('last_read', 'DATETIME'), ('last_search', 'DATETIME'), \
                ('read_count', 'INT'), ('search_count', 'INT')])
        """

        # TODO: Make other tables available?
        if name not in self._fields:

            self._fields[name] = _data.load(f'{name}.yaml')

        return self._fields[name]


    @staticmethod
    def _quotes(string: str | None, typ: str = 'VARCHAR') -> str:
        """
        Double-quotes strings to convert them to literals in SQL.

        Args:
            string:
                The string to be quoted.
            typ:
                Type of variable the string contains. Optional, defaults to
                `'VARCHAR'`.

        Returns:
            The resulting quoted string.

        Example:
            >>> cache = Cache('./')
            >>> cache._quotes('abc')
            '"abc"'
        """

        if string is None or string == 'NULL':

            return 'NULL'

        typ = typ.upper()

        if typ == 'TEXT' or isinstance(string, (set, list, tuple, dict)):

            if isinstance(string, set):

                string = list(string)

            string = f"json('{json.dumps(string)}')"

        return f'"{string}"' if (
                typ.startswith('VARCHAR') or
                typ.startswith('DATETIME')
        ) else string


    @staticmethod
    def _sqlite_type(obj: Any) -> str:
        """
        Checks a given value for the type and gives corresponding SQL
        equivalent.

        Args:
            obj:
                The value to check the type for.

        Returns:
            The resulting SQL data type as a string.

        Examples:
            >>> cache = Cache('./')
            >>> cache._sqlite_type(123)
            'INT'
            >>> cache._sqlite_type(1.25)
            'FLOAT'
            >>> cache._sqlite_type('abc')
            'VARCHAR'
        """

        pytype = type(obj).__name__

        return TYPES.get(pytype, None)


    @staticmethod
    def _typeof(value: Any) -> str:
        """
        Checks a given value for the numerical type.

        Args:
            value:
                The variable to check for the type.

        Returns:
            The resulting type as a string in SQL format. `'INT'` if the value
            is an integer or `'FLOAT'` if its a floating point number.

        Examples:
            >>> cache = Cache('./')
            >>> cache._typeof(123)
            'INT'
            >>> cache._typeof(9.01)
            'FLOAT'
            >>> cache._typeof('123')
            'INT'
        """

        if (
            isinstance(value, int)
            or (isinstance(value, str) and _misc.is_int(value))
        ):
            return 'INT'

        elif (
            isinstance(value, float)
            or (isinstance(value, str) and _misc.is_float(value))
        ):
            return 'FLOAT'


    @staticmethod
    def _where(
        uri: str | None = None,
        params: dict | None = None,
        status: Status | set[int] | None = None,
        version: int | set[int] | None = None,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
        ext: str | None = None,
        label: str | None = None,
        filename: str | None = None,
        key: str | None = None,
        include_removed: bool = False,
    ) -> str:
        """
        Generates a SQL `WHERE` clause based on different parameters defined in
        the function arguments.

        Args:
            uri:
                Uniform Resource Identifier. Optional, defaults to `None`.
            params:
                Collection of parameters in dict format where key-value pairs
                correspond to parameter-value respectively. Optional, defaults
                to `None`.
            status:
                Status(es) of the item(s). Optional, defaults to `None`.
            version:
                Version(s) of the item(s). Optional, defaults to `None`.
            newer_than:
                Date the times are required to be newer than. Optional, defaults
                to `None`.
            older_than:
                Date the times are required to be older than. Optional, defaults
                to `None`.
            ext:
                Extension of the file associated to the item(s). Optional,
                defaults to `None`.
            label:
                Label for the item (e.g. type, group, category...). Optional,
                defaults to `None`.
            filename:
                Name of the file associated to the item. Optional, defaults to
                `None`.
            key:
                Unique key name for the item. Optional, defaults to `None`.
            include_removed:
                Whether to include item(s) marked for removal. Optional,
                defaults to `False`.

        Returns:
            The query string with the WHERE clause.

        Example:
            >>> cache = Cache('./')
            >>> cache.create('test_entry')
            CacheItem[test_entry V:1 UNINITIALIZED]
            >>> cache._where('test_entry')
            ' WHERE item_id = "224eeebf8db5634d8d9b2a31755d4a97" AND status != \
                -1 AND status != -2'
        """

        where = []
        item_id = key

        if not item_id and (uri or params):

            params = params or {}

            if uri:

                params['_uri'] = uri

            item_id = CacheItem.serialize(params)

        if item_id:

            where.append(f'item_id = "{item_id}"')

        if filename:

            where.append(f'file_name = "{filename}"')

        status = _misc.to_set(status)

        if -1 not in status and not include_removed:

            where.append('status != -1')

        if -2 not in status and not include_removed:

            where.append('status != -2')

        if status:

            status = str(status).strip('{}')
            where.append(f'status IN ({status})')

        if version is not None and version != -1:

            version = str(_misc.to_set(version)).strip('{}')
            where.append(f'version IN ({version})')

        if newer_than:

            where.append(f'date > "{_utils.parse_time(newer_than)}"')

        if older_than:

            where.append(f'date < "{_utils.parse_time(older_than)}"')

        if ext:

            where.append(f'ext = "{ext}"')

        if label:

            where.append(f'label = "{label}"')

        where = f' WHERE {" AND ".join(where)}' if where else ''

        if version == -1: # TODO: Address cases where multiple items

            where += ' ORDER BY version DESC LIMIT 1'

        return  where
