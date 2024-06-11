from __future__ import annotations

import sqlite3

__all__ = [
    'Lock',
]
locked_connections = {}


class Lock:

    def __init__(self, con: sqlite3.Connection) -> None:
        """
        Keeps the SQLite database exclusively locked.
        """

        self.con = con


    def __enter__(self):

        if id(self.con) not in locked_connections:

            self.con.execute('BEGIN EXCLUSIVE TRANSACTION;')
            locked_connections[id(self.con)] = id(self)

        return self.con


    def __exit__(self, exc_type, exc_value, traceback):

        if locked_connections.get(id(self.con), None) == id(self):

            if exc_type is None:

                self.con.commit()

            else:

                self.con.rollback()

            del locked_connections[id(self.con)]
