from __future__ import annotations
import os
import datetime
import sqlite3

from cache_manager._item import CacheItem
import cache_manager.utils as _utils

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

    def __del__(self):

        self.con.close()

    def _set_path(self, path: str):

        if os.path.isdir(path):

            path = os.path.join(path, 'cache.sqlite')

        self.path = path

    def _open_sqlite(self):

        self.con = sqlite3.connect(self.path)
        self.cur = self.con.cursor()

    def _create_schema(self):

        self._open_sqlite()
        self.cur.execute('''
            CREATE TABLE IF NOT EXISTS
            main (
                id INT PRIMARY KEY,
                item_id VARCHAR,
                version_id VARCHAR,
                version INT,
                status INT,
                file_name VARCHAR,
                label VARCHAR,
                date DATE,
                ext VARCHAR
            )
        ''')

        for typ in ['varchar, int, date']:

            self.cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS
                attr_{} (
                    id VARCHAR FOREIGN KEY,
                    name VARCHAR,
                    value {}
                )
            '''.format(typ, typ.upper()),
            )

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

        for actual_typ in ['varchar, int, date']:

            q = f'SELECT * FROM main LEFT JOIN attr_{actual_typ}'
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

            if where:

                q += f' WHERE {where}'

            self.con.execute(q)

            for row in self.con.fetchall():

                key = row["version_id"]

                if key not in results:

                    results[key] = CacheItem(
                        key = row["item_id"],
                        version = row["version"],
                        status = row["status"],
                        ext = row["ext"],
                    )

                results[key].params[row["name"]] = row["value"]

        return list(results.values())


    def best(
            self,
            uri: str,
            params: dict | None = None,
            status: set[int] | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
        ) -> CacheItem | None:

        items = self.search(
            uri = uri,
            params = params,
            newer_than = newer_than,
            older_than = older_than,
        )

        items = sorted(items, key = lambda it: it['version'])

        for it in items[::-1]:

            if it['status'] in status:

                return it


    def create(
            self,
            uri: str,
            params: dict | None = None,
            attrs: dict | None = None
        ):

        items = self.search(
            uri = uri,
            params = params,
        )

        last_version = max((i['version'] for i in items), default = 0)
        new_version = last_version + 1

        CacheItem.new(
            uri,
            attrs,
            new_version,
            params
        ) 

        self._open_sqlite()
        self.cur.execute('''
            INSERT INTO
            main (
                id INT PRIMARY KEY,
                item_id VARCHAR,
                version_id VARCHAR,
                version INT,
                status INT,
                file_name VARCHAR,
                label VARCHAR,
                date DATE,
                ext VARCHAR
            )
        ''')
