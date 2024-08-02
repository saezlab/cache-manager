import pytest
import os

def test_clean_disk(test_cache):

    it = test_cache.create('test-clean-disk')

    test_cache.remove('test-clean-disk', keep_record = False)
    path = it.path

    with open(path, 'w') as fp:

        fp.write('something')

    assert os.path.exists(path)

    test_cache.clean_disk()

    assert not os.path.exists(path)
