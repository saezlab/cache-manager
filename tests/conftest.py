import pytest

__all__ = [
    'nested_dict',
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
