import pytest

import sqlite3

from cache_manager import _lock


def test_lock(test_cache):

    con0 = test_cache.con
    con0.execute('PRAGMA busy_timeout = 30')
    con1 = sqlite3.Connection(test_cache.path)


    with pytest.raises(sqlite3.OperationalError):

        with _lock.Lock(con1):

            test_cache.create('testlock')

    test_cache.create('testlock')

    assert test_cache.search('testlock')

    con0.execute('PRAGMA busy_timeout = 30000')
