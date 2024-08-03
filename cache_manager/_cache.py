from __future__ import annotations

from typing import Any
import os
import re
import shutil
import sqlite3
import datetime
import functools as ft
import collections

from pypath_common import _misc

from cache_manager._item import CacheItem
from cache_manager._status import status as _status
from cache_manager._session import _log
import cache_manager.utils as _utils
from . import _data
from ._lock import Lock

__all__ = [
    'ATTR_TYPES',
    'Cache',
    'TYPES',
]

ATTR_TYPES = ['varchar', 'int', 'datetime', 'float']

TYPES = {
    'str': 'VARCHAR',
    'int': 'INT',
    'float': 'FLOAT',
    'datetime': 'DATETIME',
}


class Cache:
    """
    The Cache class.
    """

    def __init__(self, path: str):
        """
        This is not empty.
        """

        self.con, self.cur = None, None
        self._fields = {}
        self._set_path(path)
        self._ensure_sqlite()

    def reload(self):

        modname = self.__class__.__module__
        mod = __import__(modname, fromlist = [modname.split('.')[0]])
        import importlib as imp
        imp.reload(mod)
        new = getattr(mod, self.__class__.__name__)
        setattr(self, '__class__', new)

    def __del__(self):

        if hasattr(self, 'con'):

            _log(f'Closing SQLite database path: {self.path}')
            self.con.close()

    def _set_path(self, path: str):

        if not os.path.exists(path):

            stem, ext = os.path.splitext(path)
            os.makedirs(stem if ext else path, exist_ok = True)

        if os.path.isdir(path):

            path = os.path.join(path, 'cache.sqlite')

        _log(f'Setting SQLite database path: {path}')
        self.path = path
        self.dir = os.path.dirname(self.path)

    def _open_sqlite(self):

        _log(f'Opening SQLite database: {self.path}')
        self.con = sqlite3.connect(self.path)
        self.cur = self.con.cursor()
        self._create_schema()


    def _ensure_sqlite(self):

        if self.con is None:

            self._open_sqlite()

    def _execute(self, query: str):

        query = re.sub(r'\s+', ' ', query)
        _log(f'Executing query: {query}')
        self.cur.execute(query)
        self.con.commit()

    def _create_schema(self):

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
                    name VARCHAR,
                    value {},
                    FOREIGN KEY(id) REFERENCES main(id)
                )
            '''.format(typ, typ.upper()),
            )


    @staticmethod
    def _quotes(string: str | None, typ: str = 'VARCHAR') -> str:
        if string is None:
            return 'NULL'

        typ = typ.upper()

        return f'"{string}"' if (
                typ.startswith('VARCHAR') or
                typ.startswith('DATETIME')
        ) else string


    @staticmethod
    def _typeof(value: Any):

        if isinstance(value, float) or _misc.is_int(value):
            return 'INT'

        elif isinstance(value, float) or _misc.is_float(value):
            return 'FLOAT'


    def _table_fields(self, name: str = 'main') -> dict[str, str]:

        if name not in self._fields:

            self._fields[name] = _data.load(f'{name}.yaml')

        return self._fields[name]


    @staticmethod
    def _where(
        uri: str | None = None,
        params: dict | None = None,
        status: _status | set[int] | None = None,
        version: int | set[int] | None = None,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
        ext: str | None = None,
        label: str | None = None,
        filename: str | None = None,
        key: str | None = None,
        include_removed: bool = False,
    ):

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


    def by_key(self, key: str, version: int) -> CacheItem:

        _log(f'Looking up key: {key}')

        return _misc.first(self.search(key=key, version=version))


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
            include_removed: bool = False,
    ) -> list[CacheItem]:
        """
        Look up items in the cache.
        """

        _log('SEARCH')
        args = locals()
        args.pop('self')
        param_str = _utils.serialize(args)
        _log(f'Searching cache: {param_str}')
        where = self._where(**args)

        results = {}

        with Lock(self.con):

            for actual_typ in ATTR_TYPES:
                q = (
                    'SELECT * FROM main '
                    f'LEFT JOIN attr_{actual_typ} attr ON main.id = attr.id '
                    f'{where}'
                )

                self._execute(q)

                _log(f'Fetching results from attr_{actual_typ}')

                for row in self.cur.fetchall():

                    keys = (
                        tuple(self._table_fields().keys()) +
                        ('_id', 'name', 'value')
                    )
                    row = dict(zip(keys, row))
                    verid = row['version_id']

                    if verid not in results:

                        _log(f'Found version: `{verid}`')

                        results[verid] = CacheItem(
                            key = row['item_id'],
                            version = row['version'],
                            status = row['status'],
                            date = row['date'],
                            filename = row['file_name'],
                            ext = row['ext'],
                            label = row['label'],
                            _id = row['id'],
                            last_read = row['last_read'],
                            last_search = row['last_search'],
                            read_count = row['read_count'],
                            search_count = row['search_count'],
                            cache = self,
                        )

                    if row['name']:

                        results[verid].attrs[row['name']] = row['value']

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


    def best(
            self,
            uri: str,
            params: dict | None = None,
            status: int | set[int] | None = _status.READY.value,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ) -> CacheItem | None:
        """
        Selecting best version of an item
        """

        status = _misc.to_set(status)

        items = self.search(
            uri = uri,
            params = params,
            status = status,
            newer_than = newer_than,
            older_than = older_than,
        )
        # TODO: Consider also date
        items = sorted(items, key = lambda it: it.version)

        if items:

            _log(f'Best matching version: {items[-1].version}')

            return items[-1]

        _log('No version found matching criteria')


    def create(
            self,
            uri: str,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int = _status.UNINITIALIZED.value,
            ext: str | None = None,
            label: str | None = None,
            filename: str | None = None,
    ) -> CacheItem:

        self._ensure_sqlite()

        _log(f'CREATE {uri}')
        args = locals()
        args.pop('self')
        param_str = _utils.serialize(args)

        _log(f'Creating new version for item {param_str}')

        with Lock(self.con):

            _log(f'Looking up existing versions of item `{uri}`')
            items = self.search(
                uri = uri,
                params = params,
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

            q = f'SELECT id FROM main WHERE version_id = "{new.key}-{new.version}"'
            self._execute(q)
            key = self.cur.fetchone()[0]
            new._id = key

            for actual_typ in ATTR_TYPES:

                _log(f'Creating attributes in attr_{actual_typ}')

                useattrs = {
                    k: v
                    for k, v in new.attrs.items()
                    if self._sqlite_type(v) == actual_typ.upper()
                }

                if not useattrs:

                    continue

                main_fields = self._table_fields()

                values = ', '.join(
                    f'({key}, "{k}", {self._quotes(v, actual_typ)})'
                    for k, v in useattrs.items()
                    if k not in main_fields
                )

                q = (f'INSERT INTO attr_{actual_typ} ( id, name, value )  VALUES {values}')

                self._execute(q)

            _log(f'Successfully created: {new.key}-{new.version}')

        _log('END CREATE')

        return new


    @staticmethod
    def _sqlite_type(obj: Any) -> str:

        pytype = type(obj).__name__

        return TYPES.get(pytype, None)


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
    ): # Make it more safer later (avoid to delete everything accidentally)
        """
        Remove CacheItem or version
        """

        with Lock(self.con):

            items = self.search(
                uri = uri,
                params = params,
                status = status,
                version = version,
                newer_than = newer_than,
                older_than = older_than,
                key = key,
            )

            if not items:
                return

            where = ','.join(str(item._id) for item in items)
            where = f' WHERE id IN ({where})'
            new_status = _status.DELETED.value if disk else _status.TRASH.value

            q = f'UPDATE main SET status = {new_status} {where};'
            self._execute(q)

            if disk:
                self._delete_files(items)

            if not keep_record:
                self._delete_records(items)


    def _delete_records(self, items: list[int, CacheItem]):

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


    def _delete_files(self, items: list[int, CacheItem]):

        for item in items:

            if os.path.exists(item.path):

                _log(f'Deleting from disk: `{item.path}`.')
                os.remove(item.path)


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
        Update one or more items.
        """

        with Lock(self.con):

            items = self.search(
                uri = uri,
                params = params,
                status = status,
                version=version,
                newer_than = newer_than,
                older_than = older_than,
                key = key,
            )

            update = update or {}
            main_fields = self._table_fields()
            main = ', '.join(
                f'{k} = {self._quotes(v, main_fields[k])}'
                for k, v in update.items() if k in main_fields
            )
            ids = [it._id for it in items]
            _log(f'Updating {len(ids)} items')
            where = f' WHERE id IN ({", ".join(map(str, ids))})'
            q = f'UPDATE main SET {main}{where};'
            self._execute(q)

            for actual_typ in ATTR_TYPES:

                _log(f'Updating attributes in attr_{actual_typ}')

                values = ', '.join(
                    f'{k} = {self._quotes(v, main_fields[k])}'
                    for k, v in update.items()
                    if (
                        k not in main_fields and
                        str(type(v)) == actual_typ
                    )
                )

                if not values: continue

                q = f'UPDATE attr_{actual_typ} SET ({values}) {where}'

                self._execute(q)

            _log(f'Finished updating attributes')


    def best_or_new(
        self,
        uri: str,
        params: dict | None = None,
        status: set[int] | None = _status.READY.value,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
        attrs: dict | None = None,
        ext: str | None = None,
        label: str | None = None,
        new_status: int = _status.WRITE.value,
        filename: str | None = None,
    ) -> CacheItem:

        args = locals()
        args.pop('self')
        args['status'] = args.pop('new_status')
        args.pop('newer_than')
        args.pop('older_than')

        with Lock(self.con):

            item = self.best(
                uri = uri,
                params = params,
                status = status,
                newer_than = newer_than,
                older_than = older_than,
            )

            if not item:

                item = self.create(**args)

        return item


    def update_status(
        self,
        uri: str | None = None,
        params: dict | None = None,
        version: int | None = -1,
        status: int = _status.READY.value,
        key: str | None = None,
    ):

        self.update(
            uri = uri,
            params = params,
            version = version,
            update = {'status': status},
            key = key,
        )


    def move_in(
        self,
        path: str,
        uri: str | None = None,
        params: dict | None = None,
        attrs: dict | None = None,
        status: int = _status.WRITE.value,
        ext: str | None = None,
        label: str | None = None,
        filename: str | None = None,
    ) -> CacheItem:

        args = locals()
        args.pop('self')
        args.pop('path')

        uri = uri or os.path.basename(path)

        item = self.create(**args)
        _log(f'Copying `{path}` to `{item.path}`.')
        shutil.copy(path, item.path)

        return item


    ready = ft.partialmethod(update_status, status = _status.READY.value)
    failed = ft.partialmethod(update_status, status = _status.FAILED.value)


    def _accessed(self, item_id: int):

        q = (
            'UPDATE main SET '
            'last_read = DATETIME("now"), read_count = read_count + 1 '
            f'WHERE id = {item_id};'
        )
        self._execute(q)


    def contents(self):

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


    def clean_disk(self):
        """
        Remove items on disk, which doesn't have any DB record
        """

        _log('Cleaning disk: removing items without DB record.')

        fnames = {
            os.path.join(self.dir, fname) for item in self.contents().values()
            if (fname := item['disk_fname']) and
            not item.get('status', False)
        }

        _log(f'Deleting {len(fnames)} files.')

        for file in fnames:

            _log(f'Deleting from disk: `{file}`.')
            os.remove(file)

        _log('Cleaning disk complete.')


    def clean_db(self):
        """
        Remove records without file on disk
        """

        _log(
            'Cleaning cache database: removing records '
            'without file on the disk.'
        )

        items = {
            item
            for it in self.contents().values()
            if (item := it['item']) and
            not os.path.exists(it['item'].path)
        }
        _log(f'Deleting {len(items)} records.')

        self._delete_records(items)
        _log('Cleaning cache database complete.')


    def autoclean(self):
        """
        Keep only ready/in writing items and for each item the best version
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
                if it._status in {_status.READY.value, _status.WRITE.value}
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


    def __len__(self):

        self._ensure_sqlite()

        return self.cur.execute('SELECT COUNT(*) FROM main').fetchone()[0]
