import os
import datetime
import tempfile

import pytest

from cachedir import _item, utils
from cachedir._status import Status

__all__ = [
    'TestCache',
]


def _keys(items):

    keys = {it.key for it in items}

    return keys


class TestCache:

    def test_create(self, test_cache):
        hashname = utils.hash({'_uri': 'testdb'})
        test_cache.create('testdb')
        test_cache._execute('SELECT * FROM main')
        keys = {it[1] for it in test_cache.cur.fetchall()}

        assert hashname in keys


    def test_search(self, test_cache):
        hashname = utils.hash({'_uri': 'testsearch'})
        test_cache.create('testsearch')
        items = test_cache.search('testsearch')
        items = {it.key for it in items}

        assert hashname in items


    def test_search_by_date(self, test_cache):

        hashname = utils.hash({'_uri': 'searchdate'})
        test_cache.create('searchdate')
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
            'ext': 'tsv',
            'label': 'testlabel',
        }

        hashname = utils.hash({'_uri': 'searchmain'})
        test_cache.create('searchmain', **args)

        search_args = [
            {'status': 0},
            {'ext': 'tsv'},
            {'label': 'testlabel'},
            {'status': 0, 'ext': 'tsv'},
            {'label': 'testlabel', 'ext': 'tsv'},
        ]

        for args in search_args:

            status_search = test_cache.search('searchmain', **args)

            assert hashname in _keys(status_search)

        search_args = [
            {'status': 99},
            {'ext': 'xml'},
            {'label': 'testla'},
            {'status': 99, 'ext': 'tsv'},
            {'label': 'testlabel', 'ext': 'csv'},
        ]

        for args in search_args:

            status_search = test_cache.search('searchmain', **args)

            assert hashname not in _keys(status_search)


    def test_best_or_new(self, test_cache):

        it = test_cache.best_or_new('bestornew', attrs = {'foo': 'bar'})

        assert isinstance(it, _item.CacheItem)
        assert it.status == 1
        assert it.version == 1

        it = test_cache.best_or_new('bestornew', status = 1)

        assert isinstance(it, _item.CacheItem)
        assert it.status == 1
        assert it.version == 1
        assert it.attrs == {'foo': 'bar'}
        assert it.params == {'_uri': 'bestornew'}

        it = test_cache.best_or_new('bestornew')

        assert isinstance(it, _item.CacheItem)
        assert it.status == 1
        assert it.version == 2


    def test_best_skips_ready_without_file(self, test_cache):

        # A READY item whose backing file is missing is corrupt and must not be
        # returned as the best match -- otherwise the caller gets a path to a
        # nonexistent file (the bug behind dlmachine's "all mirrors failed" on a
        # cleaned/partial cache entry).
        it = test_cache.best_or_new('readyfilecheck')
        assert it.status == Status.WRITE.value

        # No file on disk yet -> marking it READY makes it corrupt.
        test_cache.ready('readyfilecheck', version = it.version)
        assert test_cache.best('readyfilecheck') is None

        # With the file actually present, READY is a valid hit again.
        with open(it.path, 'wb') as fp:
            fp.write(b'payload')

        best = test_cache.best('readyfilecheck')
        assert best is not None
        assert best.version == it.version

        # Removing the file makes it skip the item once more.
        os.remove(it.path)
        assert test_cache.best('readyfilecheck') is None


    def test_best_keeps_writeitem_without_file(self, test_cache):

        # WRITE items are slots for an in-progress / resumable download and are
        # file-less by design, so the file-existence check must NOT exclude them.
        w = test_cache.best_or_new('writeslot')
        assert w.status == Status.WRITE.value

        found = test_cache.best('writeslot', status = Status.WRITE.value)
        assert found is not None
        assert found.version == w.version


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


    def test_update_date(self, test_cache):

        it = test_cache.best_or_new('updatedate')

        d = '2027-01-23 00:00:01'

        it.update_date(newdate = d)

        it = it._from_main()

        assert it.date == utils.parse_time(d)


    def test_update_date_attrs(self, test_cache):

        it = test_cache.best_or_new('updatedateattrs')

        d = '2027-01-23 00:00:01'

        it.update_date(datefield = 'testdate', newdate = d)

        it = it._from_main()

        assert it.attrs['testdate'] == utils.parse_time(d)


    def test_item_status(self, test_cache):

        it = test_cache.best_or_new('itemstatus')

        assert it.status == 1
        assert it._status == 1

        it.status = 3

        assert it.status == 3
        assert it._status == 3



    def test_move_in(self, test_cache):

        with tempfile.NamedTemporaryFile() as tmpfile:
            content = b'Test tmp file'
            tmpfile.write(content)
            tmpfile.file.flush()

            item = test_cache.move_in(tmpfile.name)

            with open(item.path, 'rb') as fp:

                assert fp.read() == content
