import os

from cache_manager import utils

__all__ = [
    'TestRemove',
]


class TestRemove:

    def test_trashbin(self, test_cache):

        it = test_cache.create('testremove')
        with open(it.path, 'w') as f:
            f.write('Something')

        assert os.path.exists(it.path)

        test_cache.remove('testremove')
        its = test_cache.search('testremove', include_removed=True)
        status = {it._status for it in its}

        assert status == {-1}
        assert os.path.exists(it.path)

    def test_remove_from_disk(self, test_cache):
        it = test_cache.create('testremove2')

        with open(it.path, 'w') as f:
            f.write('Something')

        assert os.path.exists(it.path)

        test_cache.remove('testremove2', disk = True)
        its = test_cache.search('testremove2', include_removed=True)
        status = {it._status for it in its}

        assert status == {-2}
        assert not os.path.exists(it.path)


    def test_search_removed(self, test_cache):

        hashname = utils.hash({'_uri': 'testremove3'})
        test_cache.create('testremove3')
        test_cache.remove('testremove3')
        its = test_cache.search('testremove3')

        assert not its

        test_cache._execute('SELECT item_id, status FROM main')
        keys = {it[0] for it in test_cache.cur.fetchall() if it[1] != -1}

        assert hashname not in keys


    def test_item_remove(self, test_cache):
        it = test_cache.best_or_new('itemremove4')

        assert test_cache.search('itemremove4')

        it.remove()

        assert not test_cache.search('itemremove4')

    def test_removed_all(self, test_cache):
        it = test_cache.create('testremove5')

        with open(it.path, 'w') as f:
            f.write('Something')

        assert os.path.exists(it.path)

        test_cache.remove('testremove5', disk = True, keep_record=False)
        its = test_cache.search('testremove5', include_removed=True)
        status = {it._status for it in its}

        assert status == set()
        assert not os.path.exists(it.path)
