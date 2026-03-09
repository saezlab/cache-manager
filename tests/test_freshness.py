from pathlib import Path

from cache_manager import _freshness


class _DummyItem:

    def __init__(self, attrs):
        self.attrs = attrs


def test_metadata_from_item_extracts_headers():

    item = _DummyItem(
        attrs={
            'resp_headers': {
                'ETag': 'abc',
                'Last-Modified': 'Wed, 21 Oct 2015 07:28:00 GMT',
            },
            'sha256': 'x',
            'size': 10,
        }
    )

    meta = _freshness.metadata_from_item(item)

    assert meta['etag'] == 'abc'
    assert meta['last_modified'] == 'Wed, 21 Oct 2015 07:28:00 GMT'
    assert meta['sha256'] == 'x'
    assert meta['size'] == 10


def test_check_freshness_size(tmp_path: Path):

    path = tmp_path / 'a.txt'
    path.write_text('hello')

    is_current, reason = _freshness.check_freshness(
        local_path=path,
        remote_headers={'Content-Length': str(path.stat().st_size)},
        local_metadata={},
        method='size',
    )

    assert is_current is True
    assert reason == 'size match'
