import pytest

import datetime


def test_last_search(test_cache):

    it = test_cache.create('testlastsearch')
    t0 = datetime.datetime.now().replace(microsecond=0)# - datetime.timedelta(1)

    test_cache.search('testlastsearch')

    q = f'SELECT datetime(last_search, "localtime"), search_count FROM main WHERE id = {it._id};'
    test_cache._execute(q)
    last, count = test_cache.cur.fetchone()

    t1 = datetime.datetime.strptime(last, '%Y-%m-%d %H:%M:%S')

    assert t1 >= t0
    assert int(count) == 1
