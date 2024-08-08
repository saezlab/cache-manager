
def test_search_by_attrs(test_cache):

    it = test_cache.best_or_new('search_by_attrs', attrs = {'foo': 'something'})

    result = test_cache.search(attrs = {'foo': 'something'})

    assert result[0].key == it.key


def test_search_by_attrs2(test_cache):

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
