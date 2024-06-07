import pytest

from cache_manager import Cache

__all__ = [
    'nested_dict',
    'test_cache',
]


@pytest.fixture
def nested_dict():

    return {
        'foo': [99, 44, 77],
        'bar': {
            'x': 45.231234124,
            'y': 89.2323232,
        },
        'baz': None,
        None: 'foobar',
    }


@pytest.fixture(scope='session')
def test_cache(tmpdir_factory):
    fn = tmpdir_factory.mktemp('test_cache')
    c = Cache(fn)

    return c
