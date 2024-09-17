def test_attr_update_new(test_cache):

    attrs0 = {'foo': 'bar'}
    it = test_cache.best_or_new('testattrupdate', attrs = attrs0)

    attrs1 = {'foonew': 'barnew'}
    test_cache.update(it.uri, update = {'attrs': attrs1})

    it1 = test_cache.search('testattrupdate')[0]

    assert it1.attrs == {**attrs0, **attrs1}


def test_update_attr_namespaces(test_cache):

    attrs0 = {
        'foo': 'bar',
        'attrdict': {
            'numbers': [0, 1, 2, 3, 4]
        }
    }

    it = test_cache.best_or_new('updatenamespace', attrs = attrs0)
    it1 = test_cache.search('updatenamespace')[0]

    assert it1.attrs == attrs0

    attrs1 = {
        'foo': 'bar',
        'attrdict': {
            'numbers': [5, 6, 7, 8]
        }
    }
    test_cache.update(it.uri, update = {'attrs': attrs1})
    it2 = test_cache.search('updatenamespace')[0]

    assert it2.attrs == attrs1
