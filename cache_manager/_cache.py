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
                    value {}
                )
            '''.format(typ, typ.upper()),
            )

    def search(
            self,
            uri: str,
            attrs: dict | None = None,
            status: int | None = None,
            newer_than: str | datetime.datetime | None = None,
            older_than: str | datetime.datetime | None = None,
        ) -> list[CacheItem]:
        """
        Look up items in the cache.
        """

        q = ' SELECT item_id, version, status, date, ext, label FROM main'
        where = ''

        if uri or attrs:

            item_id = CacheItem.serialize(uri, attrs)

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

        return [
            CacheItem(*row)
            for row in self.con.fetchall()
        ]
