import pytest

from cache_manager import utils

__all__ = ['TestUtils']


class TestUtils:

    def test_serialize(self, nested_dict):

        serial = utils.serialize(nested_dict)

        assert serial == (
            '[None=foobar,bar=[x=45.231234124,y=89.2323232],'
            'baz=None,foo=[44,77,99]]'
        )

    def test_hash(self):

        assert utils.hash(124) == 'c8ffe9a587b126f152ed3d89a146b445'
