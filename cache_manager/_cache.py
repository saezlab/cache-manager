import os
import sqlite3

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
                extension VARCHAR
            )
        ''')

        for typ in ['varchar, int, date']:
            self.cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS
                attr_{} (
                    version_id VARCHAR FOREIGN KEY,
                    value {}
                )
            '''.format(typ, typ.upper()),
            )
