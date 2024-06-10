import pytest

import datetime

from cache_manager import utils


def _keys(items):

    keys = {it.key for it in items}

    return keys


class TestCache:

    def test_create(self, test_cache):
        hashname = utils.hash({"_uri": "testdb"})
        test_cache.create("testdb")
        test_cache._execute("SELECT * FROM main")
        keys = {it[1] for it in test_cache.cur.fetchall()}

        assert hashname in keys


    def test_search(self, test_cache):
        hashname = utils.hash({"_uri": "testsearch"})
        test_cache.create("testsearch")
        items = test_cache.search("testsearch")
        items = {it.key for it in items}

        assert hashname in items


    def test_search_by_date(self, test_cache):

        hashname = utils.hash({"_uri": "searchdate"})
        test_cache.create("searchdate")
        keys = lambda items: {it.key for it in items}

        older_than = test_cache.search(
            'searchdate',
            older_than = datetime.datetime.now() + datetime.timedelta(2),
        )
        newer_than = test_cache.search(
            'searchdate',
            newer_than = datetime.datetime.now() - datetime.timedelta(100),
        )

        assert hashname in keys(older_than)
        assert hashname in keys(newer_than)

        older_than = test_cache.search(
            'searchdate',
            newer_than = datetime.datetime.now() + datetime.timedelta(2),
        )
        newer_than = test_cache.search(
            'searchdate',
            older_than = datetime.datetime.now() - datetime.timedelta(100),
        )
        assert hashname not in keys(older_than)
        assert hashname not in keys(newer_than)


    def test_search_by_main_fields(self, test_cache):

        args = {
            'status': 0,
            'ext': '.tsv',
            'label': 'testlabel'
        }

        hashname = utils.hash({"_uri": "searchmain"})
        test_cache.create("searchmain", **args)

        search_args = [
            {'status': 0},
            {'ext': '.tsv'},
            {'label': 'testlabel'}
        ]

        for args in search_args:

            status_search = test_cache.search('searchmain', **args)

            assert hashname in _keys(status_search)


    def test_remove(self, test_cache):
        hashname = utils.hash({"_uri": "testremove"})
        test_cache.create("testremove")
        test_cache.remove("testremove")
        test_cache._execute("SELECT * FROM main")
        keys = {it[1] for it in test_cache.cur.fetchall()}

        assert hashname not in keys
