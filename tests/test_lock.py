from cache_manager import _lock

import sqlite3

def test_lock(test_cache):

    con0 = test_cache.con
    con0.execute('PRAGMA busy_timeout = 30')
    con1 = sqlite3.Connection(test_cache.path)

    with _lock.Lock(con1):

        test_cache.create('testlock')

    con0.execute('PRAGMA busy_timeout = 30000')
