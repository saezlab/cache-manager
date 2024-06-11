from cache_manager import _lock


def test_lock(test_cache):

    con0 = test_cache.con
    con1 = sqlite3.Connection(test_cache.path)

    with _lock.Lock(con1):

        test_cache.create('testlock')

