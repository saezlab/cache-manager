import pytest
from cache_manager import utils


class TestCache:

    def test_create(self, test_cache):
        hashname = utils.hash({"_uri": "testdb"})
        test_cache.create("testdb")
        test_cache._execute("SELECT * FROM main")
        keys = {it[1] for it in test_cache.cur.fetchall()}

        assert hashname in keys
