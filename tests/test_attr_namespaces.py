

def test_attr_namespaces(test_cache):

    params = {
        'desc_args': {
            'query': {'foofoo': 'bar', 'page': 23},
            'post': 'abcdefg',
        },
    }
    attrs = {
        'foofoo': 'bar',
        'bar': {
            'baz': 'qux',
            'qux': {
                'quux': 'corge',
            },
        },
    }

    it = test_cache.best_or_new('attr_namespaces', params = params, attrs = attrs)

    result = test_cache.search('attr_namespaces', params = params)

    assert len(result) == 1

    it0 = result[0]

    assert it0.key == it.key
    assert it0.params == it.params

    result = test_cache.search(
        'attr_namespaces',
        params = params,
        attrs = {'baz': 'qux'},
    )

    assert len(result) == 1

    result = test_cache.search(
        'attr_namespaces',
        params = params,
        attrs = {'quux': 'corge'},
    )

    assert len(result) == 0
