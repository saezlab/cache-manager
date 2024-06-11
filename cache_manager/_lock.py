from __future__ import annotations

import sqlite3

__all__ = [
    'Lock',
]

class Lock:

    def __init__(self, con: sqlite3.Connection) -> None:
        """
        Keeps the SQLite database exclusively locked.
        """

        self.con = con


    def __enter__(self):

        self.con.execute('BEGIN EXCLUSIVE TRANSACTION;')

        return self.con


    def __exit__(self, exc_type, exc_value, traceback):

        if exc_type is None:

            self.con.commit()

        else:

            self.con.rollback()

        self.con.close()
