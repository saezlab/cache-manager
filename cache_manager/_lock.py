from __future__ import annotations

import sqlite3

__all__ = [
    'Lock',
]
locked_connections = {}


class Lock:
    """
    Context manager that keeps the SQLite database exclusively locked. This
    avoids concurrency when performing changes within a database connection.
    """

    def __init__(self, con: sqlite3.Connection) -> None:
        """
        Args:
            con:
                The current instance of `sqlite3.Connection` to be locked.
        """

        self.con = con


    def __enter__(self):
        """
        Enters the context manager initiating a SQL exclusive transaction.
        """

        if id(self.con) not in locked_connections:

            self.con.execute('BEGIN EXCLUSIVE TRANSACTION;')
            locked_connections[id(self.con)] = id(self)

        return self.con


    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exits the context manager closing the exclusive transaction and
        unlocking the connection. If an exception occurs, changes will be rolled
        back and the connection will be unlocked.

        Args:
            exc_type:
                Exception type if any occurs inside the context.
            exc_value:
                Value of the exception if any occurs inside the context.
            traceback:
                Traceback of the exception if any occurs inside the context.
        """

        if locked_connections.get(id(self.con), None) == id(self):

            if exc_type is None:

                self.con.commit()

            else:

                self.con.rollback()

            del locked_connections[id(self.con)]
