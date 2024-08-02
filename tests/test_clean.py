import pytest
import os

from cache_manager import _status


def test_clean_disk(test_cache):

    it = test_cache.create('test-clean-disk')

    test_cache.remove('test-clean-disk', keep_record = False)
    path = it.path

    with open(path, 'w') as fp:

        fp.write('something')

    assert os.path.exists(path)

    test_cache.clean_disk()

    assert not os.path.exists(path)


def test_clean_db(test_cache):

    it = test_cache.create('test-clean-db')
    test_cache.clean_db()

    assert not os.path.exists(it.path)

    search = test_cache.search('test-clean-db', include_removed = True)

    assert len(search) == 0


def test_autoclean(test_cache):

    items = [
        test_cache.create('test-autoclean')
        for _ in range(3)
    ]

    items[1].status = _status.status.READY.value

    test_cache.autoclean()

    items = test_cache.search('test-autoclean', include_removed = True)

    assert len(items) == 1
    assert items[0].version == 2
    assert items[0].status == _status.status.READY.value
