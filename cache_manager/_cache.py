import sqlite3
import os


class Cache:

    def __init__(self, path: str):

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
