import pytest

import datetime


def test_last_search(test_cache):

    it = test_cache.create('testlastsearch')
    t0 = datetime.datetime.now()

    test_cache.search('testlastsearch')

    q = f'SELECT last_search, search_count FROM main WHERE id = {it._id};'
    last = test_cache._execute(q).fetchone()[0]
    t1 = datetime.datetime.strptime(last[0], '%Y-%m-%d %H:%M:%S')

    assert t1 > t0
    assert int(last[1]) == 1
