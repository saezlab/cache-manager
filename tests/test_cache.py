import pytest

import datetime
import tempfile

from cache_manager import utils
from cache_manager import _item


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
            {'label': 'testlabel'},
            {'status': 0, 'ext': '.tsv'},
            {'label': 'testlabel', 'ext': '.tsv'},
        ]

        for args in search_args:

            status_search = test_cache.search('searchmain', **args)

            assert hashname in _keys(status_search)

        search_args = [
            {'status': 99},
            {'ext': '.xml'},
            {'label': 'testla'},
            {'status': 99, 'ext': '.tsv'},
            {'label': 'testlabel', 'ext': '.csv'},
        ]

        for args in search_args:

            status_search = test_cache.search('searchmain', **args)

            assert hashname not in _keys(status_search)


    def test_remove(self, test_cache):
        hashname = utils.hash({"_uri": "testremove"})
        test_cache.create("testremove")
        test_cache.remove("testremove")
        test_cache._execute("SELECT * FROM main")
        keys = {it[1] for it in test_cache.cur.fetchall()}

        assert hashname not in keys


    def test_best_or_new(self, test_cache):

        it = test_cache.best_or_new('bestornew', attrs = {'foo': 'bar'})

        assert isinstance(it, _item.CacheItem)
        assert it.status == 1
        assert it.version == 1

        it = test_cache.best_or_new('bestornew', status = 1)

        assert isinstance(it, _item.CacheItem)
        assert it.status == 1
        assert it.version == 1
        assert it.attrs == {'foo': 'bar', '_uri': 'bestornew'}

        it = test_cache.best_or_new('bestornew')

        assert isinstance(it, _item.CacheItem)
        assert it.status == 1
        assert it.version == 2


    def test_update_status(self, test_cache):

        it = test_cache.create('teststatus')

        assert it.status == 0

        it = test_cache.best_or_new('teststatus')

        assert it.status == 1

        test_cache.update_status('teststatus')
        its = test_cache.search('teststatus')

        assert {(it.version, it.status) for it in its} == {(1, 0), (2, 3)}

        test_cache.update_status('teststatus', status = 2, version = 1)
        its = test_cache.search('teststatus')

        assert {(it.version, it.status) for it in its} == {(1, 2), (2, 3)}

        test_cache.failed('teststatus', version = 2)
        its = test_cache.search('teststatus')

        assert all(it.status == 2 for it in its)

        test_cache.ready('teststatus', version = 1)
        its = test_cache.search('teststatus')

        assert {(it.version, it.status) for it in its} == {(1, 3), (2, 2)}


    def test_item_status(self, test_cache):

        it = test_cache.best_or_new('itemstatus')

        assert it.status == 1
        assert it._status == 1

        it.status = 3

        assert it.status == 3
        assert it._status == 3


    def test_item_remove(self, test_cache):

        it = test_cache.best_or_new('itemremove')

        assert test_cache.search('itemremove')

        it.remove()

        assert not test_cache.search('itemremove')


    def test_move_in(self, test_cache):

        with tempfile.NamedTemporaryFile() as tmpfile:
            content = b"Test tmp file"
            tmpfile.write(content)
            tmpfile.file.flush()

            item = test_cache.move_in(tmpfile.name)

            with open(item.path, 'rb') as fp:

                assert fp.read() == content
