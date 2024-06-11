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

        if not self.con._locked:

            self.con.execute('BEGIN EXCLUSIVE TRANSACTION;')
            self.con._locked = id(self)

        return self.con


    def __exit__(self, exc_type, exc_value, traceback):

        if self.con._locked == id(self):

            if exc_type is None:

                self.con.commit()

            else:

                self.con.rollback()

            self.con._locked = False
