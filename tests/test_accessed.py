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


def test_last_read(test_cache):

    it = test_cache.create('testlastread')
    t0 = datetime.datetime.now().replace(microsecond=0)# - datetime.timedelta(1)

    q = f'SELECT datetime(last_read, "localtime"), read_count FROM main WHERE id = {it._id};'
    test_cache._execute(q)
    last, count = test_cache.cur.fetchone()

    assert count == 0
    assert last is None

    with open(it.path, 'w') as f:
        f.write("Something")

    it._open()
    q = f'SELECT datetime(last_read, "localtime"), read_count FROM main WHERE id = {it._id};'
    test_cache._execute(q)
    last, count = test_cache.cur.fetchone()

    t1 = datetime.datetime.strptime(last, '%Y-%m-%d %H:%M:%S')

    assert t1 >= t0
    assert int(count) == 1
