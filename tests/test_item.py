import os

import pytest

def test_size(test_cache):
    it = test_cache.create('test-file-size')
    path = it.path

    assert it.size is None

    with open(path, 'w') as fp:
        fp.write('something')

    assert os.path.exists(path)
    assert it.size > 8
