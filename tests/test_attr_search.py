
def test_search_by_attrs(test_cache):

    it = test_cache.best_or_new('search_by_attrs', attrs = {'foo': 'bar'})

    result = test_cache.search(attrs = {'foo': 'bar'})

    assert result[0].key == it.key
