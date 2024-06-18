from __future__ import annotations

from typing import Any
import os
import sqlite3
import datetime
import functools as ft

from pypath_common import _misc

from cache_manager._item import CacheItem
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


    @staticmethod
    def _table_fields(name: str = 'main') -> dict[str, str]:

        return _data.load(f'{name}.yaml')


    @staticmethod
    def _where(
        uri: str | None = None,
        params: dict | None = None,
        status: int | set[int] | None = None,
        version: int | set[int] | None = None,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
        ext: str | None = None,
        label: str | None = None,
        key: str | None = None,
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

        if status is not None:
            status = str(_misc.to_set(status)).strip('{}')
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
            key: str | None = None,
    ) -> list[CacheItem]:
        """
        Look up items in the cache.
        """

        args = locals()
        args.pop('self')
        param_str = _utils.serialize(args)
        _log(f'Searching cache: {param_str}')

        results = {}

        with Lock(self.con):

            for actual_typ in ATTR_TYPES:
                q = f'SELECT * FROM main LEFT JOIN attr_{actual_typ}'
                q += self._where(**args)

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

                        results[verid] = CacheItem(
                            key = row['item_id'],
                            version = row['version'],
                            status = row['status'],
                            ext = row['ext'],
                            _id = row['id'],
                            cache = self,
                        )

                    if row['name']:

                        results[verid].attrs[row['name']] = row['value']

        _log(f'Retrieved {len(results)} results')

        return list(results.values())


    def best(
            self,
            uri: str,
            params: dict | None = None,
            status: int | set[int] | None = 3,
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
            status: int = 0,
            ext: str | None = None,
            label: str | None = None,
    ) -> CacheItem:

        self._ensure_sqlite()

        args = locals().pop('self')
        param_str = _utils.serialize(args)

        _log(f'Creating new version for item {param_str}')

        with Lock(self.con):

            items = self.search(
                uri = uri,
                params = params,
            )

            last_version = max((i.version for i in items), default = 0)

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
                    ext
                )
                VALUES (
                    {self._quotes(new.key)},
                    "{new.key}-{new.version}",
                    {new.version},
                    {new.status},
                    {self._quotes(new.filename)},
                    {self._quotes(new.label)},
                    {self._quotes(new.date)},
                    {self._quotes(new.ext)}
                )
            ''')

            q = f'SELECT id FROM main WHERE version_id = "{new.key}-{new.version}"'
            self._execute(q)
            key = self.cur.fetchone()[0]

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

        return new


    @staticmethod
    def _sqlite_type(obj: Any) -> str:

        pytype = type(obj).__name__

        return TYPES.get(pytype, None)


    def remove(
            self,
            uri: str | None = None,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int | None = None,
            ext: str | None = None,
            label: str | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ): # Make it more safer later (avoid to delete everything accidentally)
        """
        Remove CacheItem or version
        """

        with Lock(self.con):

            items = self.search(
                uri = uri,
                params = params,
                status = status,
                newer_than = newer_than,
                older_than = older_than,
            )

            if not items:
                return

            where = ','.join(str(item._id) for item in items)
            where = f' WHERE id IN ({where})'

            for actual_typ in ATTR_TYPES:
                attr_table = f'attr_{actual_typ}'

                _log(f'Deleting attributes from {attr_table}')

                q = f'DELETE FROM {attr_table} {where}'

                self._execute(q)

            q = f'DELETE FROM main'
            q += where

            self._execute(q)

            _log(f'Deleted {len(items)} results')


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
        status: set[int] | None = 3,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
        attrs: dict | None = None,
        ext: str | None = None,
        label: str | None = None,
        new_status: int = 1,
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
        status: int = 3,
        key: str | None = None,
    ):

        self.update(
            uri = uri,
            params = params,
            version = version,
            update = {'status': status},
            key = key,
        )



    ready = ft.partialmethod(update_status, status = 3)
    failed = ft.partialmethod(update_status, status = 2)
