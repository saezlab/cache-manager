def test_attr_update_new(test_cache):

    attrs0 = {'foo': 'bar'}
    it = test_cache.best_or_new('testattrupdate', attrs = attrs0)

    attrs1 = {'foonew': 'barnew'}
    test_cache.update(it.key, update = {'attrs': attrs1})

    it1 = test_cache.best_or_new('testattrupdate')

    assert it1.attrs == {**attrs0, **attrs1}
