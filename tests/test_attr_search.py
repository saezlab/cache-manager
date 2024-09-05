from dateutil.parser import parse as dateparse


from cache_manager import _status

__all__ = [
    'test_search_by_attrs_datetime',
    'test_search_by_attrs_extended',
    'test_search_by_attrs_int',
    'test_search_by_attrs_simple',
]


def test_search_by_attrs_simple(test_cache):

    it = test_cache.best_or_new('search_by_attrs', attrs = {'foo': 'something'})

    result = test_cache.search(attrs = {'foo': 'something'})

    assert result[0].key == it.key


def test_search_by_attrs_extended(test_cache):

    it0 = test_cache.best_or_new('wrong-one-1', attrs = {'foo': 'baz'})
    it1 = test_cache.best_or_new('wrong-one-2', attrs = {'foo': 'bar'})
    it2 = test_cache.best_or_new('good-one', attrs = {'foo': 'bar'})

    result = test_cache.search('good-one', attrs = {'foo': 'bar'})

    assert len(result) == 1
    assert result[0].key == it2.key
    assert result[0].attrs['foo'] == 'bar'
    assert result[0].uri == 'good-one'

    result = test_cache.search('good-one', attrs = {'foo': 'baz'})

    assert len(result) == 0

    result = test_cache.search(attrs = {'foo': 'bar'})

    assert len(result) == 2
    assert {r.uri for r in result} == {'good-one', 'wrong-one-2'}


def test_search_by_attrs_int(test_cache):
    it = test_cache.best_or_new('search_by_attrs_int', attrs = {'foo': 123})

    result = test_cache.search(attrs = {'foo': 123})

    assert result[0].key == it.key

    result = test_cache.search(attrs = {'foo<=': 500})

    assert result[0].key == it.key

    result = test_cache.search(attrs = {'foo>=': 500})

    assert len(result) == 0


def test_search_by_attrs_datetime(test_cache):
    it = test_cache.best_or_new(
        'search_by_attrs_datetime',
        attrs = {'foo': dateparse('2020/12/31')},
    )

    result = test_cache.search(attrs = {'foo': dateparse('2020/12/31')})

    assert result[0].key == it.key

    result = test_cache.search(attrs = {'foo<=': dateparse('2021/12/31')})

    assert result[0].key == it.key

    result = test_cache.search(attrs = {'foo>=': dateparse('2021/12/31')})

    assert len(result) == 0


def test_search_by_attrs_datetime2(test_cache):

    it = test_cache.best_or_new(
        'search_by_attrs_datetime2',
        attrs = {'foo2': dateparse('2020/12/31')},
    )

    result = test_cache.search(attrs = {'foo2': 'DATE:2020/12/31'})

    assert result[0].key == it.key

    result = test_cache.search(attrs = {'foo2<=': 'date:2021/12/31'})

    assert result[0].key == it.key

    result = test_cache.search(attrs = {'foo2>=': 'date:2021/12/31'})

    assert len(result) == 0


def test_json_attrs(test_cache):

    attrs = {
        'dict_attr': {'foo': 'bar'},
        'list_attr': [1, 2, 3],
        'tuple_attr': (1, 2, 3),
        'set_attr': {1, 2, 3}
    }

    it = test_cache.best_or_new('blob_attrs', attrs = attrs)

    result = test_cache.search('blob_attrs')[0]

    assert result.attrs['dict_attr'] == attrs['dict_attr']
    assert result.attrs['list_attr'] == attrs['list_attr']
    assert result.attrs['tuple_attr'] == list(attrs['tuple_attr'])
    assert set(result.attrs['set_attr']) == attrs['set_attr']


def test_search_params(test_cache):

    params = {
        'desc_args': {
            'query': {'foo': 'bar', 'page': 23},
            'post': 'abcdefg',
        },
    }
    attrs = {
        'foo': 'bar',
    }

    it0 = test_cache.best_or_new(
        'search_params',
        params = params,
        attrs = attrs,
        new_status = _status.Status.READY.value,
    )
    it1 = test_cache.best_or_new('search_params', params = params)

    its = test_cache.search('search_params')

    assert len(its) == 0

    its = test_cache.search('search_params', params = params)

    assert len(its) == 1
