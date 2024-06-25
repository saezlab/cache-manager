import pytest

class TestRemove:

    def test_remove(self, test_cache):
        test_cache.create('testremove')
        test_cache.remove('testremove')
        its = test_cache.search('testremove', include_removed=True)
        status = {it._status for it in its}

        assert status == {-1}

    def test_remove(self, test_cache):
        hashname = utils.hash({"_uri": "testremove"})
        test_cache.create("testremove")
        test_cache.remove("testremove")
        test_cache._execute("SELECT * FROM main")
        keys = {it[1] for it in test_cache.cur.fetchall()}

        assert hashname not in keys


    def test_item_remove(self, test_cache):

        it = test_cache.best_or_new('itemremove')

        assert test_cache.search('itemremove')

        it.remove()

        assert not test_cache.search('itemremove')
