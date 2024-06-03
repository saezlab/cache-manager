from __future__ import annotations

import os
import sqlite3
import datetime

from cache_manager._session import _log
from cache_manager._item import CacheItem
import cache_manager.utils as _utils
from . import _data

__all__ = [
    'Cache',
]


class Cache:
    """
    The Cache class.
    """

    def __init__(self, path: str):
        """
        This is not empty.
        """

        self._set_path(path)
        self._open_sqlite()

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

    def _execute(self, query: str):

        _log(f'Executing query: {query}')
        self.cur.execute(query)

    def _create_schema(self):

        self._open_sqlite()

        _log(f'Initializing new database')

        fields = ",".join(f'{k}: {v}' for k, v in self._table_fields())

        _log(f'Creating main table')
        self._execute(f'''
            CREATE TABLE IF NOT EXISTS
            main (
                {fields}
            )
        ''')

        for typ in ['varchar, int, date']:

            _log(f'Creating attr_{typ} table')
            self._execute(
                '''
                CREATE TABLE IF NOT EXISTS
                attr_{} (
                    id VARCHAR FOREIGN KEY,
                    name VARCHAR,
                    value {}
                )
            '''.format(typ, typ.upper()),
            )

    @staticmethod
    def _table_fields(name: str = 'main') -> dict[str, str]:

        return _data.load(f'{name}.yaml')


    @staticmethod
    def _where(
        uri: str,
        params: dict | None = None,
        status: int | None = None,
        newer_than: str | datetime.datetime | None = None,
        older_than: str | datetime.datetime | None = None,
    ):

        where = ''

        if uri or params:

            item_id = CacheItem.serialize(uri, params)

            where += f' item_id = "{item_id}"'

        if status is not None:

            where  += f' AND status = "{status}"'

        if newer_than:

            where += f' AND date > "{_utils.parse_time(newer_than)}"'

        if older_than:

            where += f' AND date < "{_utils.parse_time(older_than)}"'

        return  f' WHERE {where}' if where else ''


    def search(
            self,
            uri: str,
            params: dict | None = None,
            status: int | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ) -> list[CacheItem]:
        """
        Look up items in the cache.
        """

        results = {}

        param_str = ', '.join(
            f'{k}={_utils.serialize(v)}'
            for k, v in locals().items() if v and k != 'self'
        )
        _log(f'Searching cache: {param_str}')

        for actual_typ in ['varchar', 'int', 'date']:

            q = f'SELECT * FROM main LEFT JOIN attr_{actual_typ}'
            q += self._where(uri, params, status, newer_than, older_than)

            self._execute(q)

            _log(f'Fetching results from attr_{actual_typ}')

            for row in self.con.fetchall():

                key = row['version_id']

                if key not in results:

                    results[key] = CacheItem(
                        key = row['item_id'],
                        version = row['version'],
                        status = row['status'],
                        ext = row['ext'],
                    )

                results[key].params[row['name']] = row['value']

        _log(f'Retrieved {len(results)} results')

        return list(results.values())


    def best(
            self,
            uri: str,
            params: dict | None = None,
            status: set[int] | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ) -> CacheItem | None:
        """
        Selecting best version of an item
        """

        items = self.search(
            uri = uri,
            params = params,
            newer_than = newer_than,
            older_than = older_than,
        )

        items = sorted(items, key = lambda it: it['version'])

        for it in items[::-1]:

            if it['status'] in status:

                _log(f'Best matching version: {it["version"]}')

                return it

        _log('No version found matching criteria')


    def create(
            self,
            uri: str,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int = 0,
            ext: str | None = None,
            label: str | None = None,
    ):

        param_str = ', '.join(
            f'{k}={_utils.serialize(v)}'
            for k, v in locals().items() if v and k != 'self'
        )
        _log(f'Creating new version for item {param_str}')

        items = self.search(
            uri = uri,
            params = params,
        )

        last_version = max((i['version'] for i in items), default = 0)

        new = CacheItem.new(
            uri,
            params,
            attrs=attrs,
            version=last_version + 1,
            date=_utils.parse_time(),
            status=status,
            ext=ext,
            label=label,
        )

        _log(f'Next version: {new.key}-{new.version}')

        self._open_sqlite()
        self._execute(f'''
            INSERT INTO
            main (
                id,
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
                NULL,
                {new.key},
                {new.key}-{new.version},
                {new.version},
                {new.status},
                {new.filename},
                {new.label},
                {new.date},
                {new.ext}
            )
        ''')

        _log(f'Successfully created new version: {new.key}-{new.version}')

    def remove(
            self,
            uri: str,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int | None = None,
            ext: str | None = None,
            label: str | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
    ):
        """
        Remove CacheItem or version
        """

        items = self.search(
            uri = uri,
            params = params,
            status = status,
            newer_than = newer_than,
            older_than = older_than,
        )

        where = self._where(uri, params, status, newer_than, older_than)

        for actual_typ in ['varchar', 'int', 'date']:

            _log(f'Deleting attributes from attr_{actual_typ}')

            q = f'DELETE * FROM attr_{actual_typ} LEFT JOIN main'
            q += where

            self._execute(q)

        q = f'DELETE * FROM  main'
        q += where

        self._execute(q)

        _log(f'Deleted {len(items)} results')


    def update(
            self,
            uri: str,
            params: dict | None = None,
            attrs: dict | None = None,
            status: int | None = None,
            ext: str | None = None,
            label: str | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
            update: dict | None = None,
    ):
        """
        Update one or more items.
        """

        items = self.search(
            uri = uri,
            params = params,
            status = status,
            newer_than = newer_than,
            older_than = older_than,
        )

        update = update or {}



        where = self._where(uri, params, status, newer_than, older_than)

        for actual_typ in ['varchar', 'int', 'date']:

            _log(f'Updating attributes in attr_{actual_typ}')

            q = f'UPDATE attr_{actual_typ} SET (value) LEFT JOIN main'
            q += where

            self._execute(q)

        q = f'DELETE * FROM  main'
        q += where

        self._execute(q)

        _log(f'Deleted {len(items)} results')

