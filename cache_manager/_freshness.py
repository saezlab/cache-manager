"""
Freshness checking utilities for cache-backed downloads.
"""

from __future__ import annotations

import os
import hashlib
import logging
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


#--- Module logger 
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def metadata_from_item(item) -> dict:
    """
    Extracts freshness-relevant metadata from a CacheItem.
    """

    attrs = getattr(item, 'attrs', {}) or {}
    headers = attrs.get('resp_headers', {}) or {}

    return {
        'etag': attrs.get('etag') or headers.get('ETag') or headers.get('etag'),
        'last_modified': (
            attrs.get('last_modified') or
            headers.get('Last-Modified') or
            headers.get('last-modified')
        ),
        'sha256': attrs.get('sha256'),
        'size': attrs.get('size'),
    }


def check_freshness(
    local_path: str | Path,
    remote_headers: dict,
    local_metadata: dict | None = None,
    method: str = 'auto',
) -> tuple[bool, str]:

    if not os.path.exists(local_path):
        return False, 'local file does not exist'

    local_metadata = local_metadata or {}

    if method == 'auto':
        for check_method in ('etag', 'modified', 'size'):
            is_current, reason = _check_by_method(
                local_path,
                remote_headers,
                local_metadata,
                check_method,
            )
            if reason != 'method_unavailable':
                return is_current, f'{check_method}: {reason}'
        return False, 'no check method available'

    return _check_by_method(local_path, remote_headers, local_metadata, method)


def _check_by_method(
    local_path: str | Path,
    remote_headers: dict,
    local_metadata: dict,
    method: str,
) -> tuple[bool, str]:

    if method == 'etag':
        return _check_etag(remote_headers, local_metadata)
    if method == 'modified':
        return _check_last_modified(remote_headers, local_metadata)
    if method == 'hash':
        return _check_hash(local_path, remote_headers, local_metadata)
    if method == 'size':
        return _check_size(local_path, remote_headers)

    return False, f'unknown method: {method}'


def _check_etag(remote_headers: dict, local_metadata: dict) -> tuple[bool, str]:

    remote_etag = remote_headers.get('ETag') or remote_headers.get('etag')
    local_etag = local_metadata.get('etag')

    if not remote_etag:
        return False, 'method_unavailable'
    if not local_etag:
        return False, 'no local etag stored'

    is_current = remote_etag == local_etag
    logger.debug(f'ETag check: remote={remote_etag}, local={local_etag}, current={is_current}')
    return is_current, 'etag match' if is_current else 'etag mismatch'


def _check_last_modified(
    remote_headers: dict,
    local_metadata: dict,
) -> tuple[bool, str]:

    remote_modified = (
        remote_headers.get('Last-Modified') or
        remote_headers.get('last-modified')
    )
    local_modified = local_metadata.get('last_modified')

    if not remote_modified:
        return False, 'method_unavailable'
    if not local_modified:
        return False, 'no local last-modified stored'

    try:
        remote_dt = parsedate_to_datetime(remote_modified)
        local_dt = parsedate_to_datetime(local_modified)
        is_current = local_dt >= remote_dt
        logger.debug(f'Last-Modified check: remote={remote_dt}, local={local_dt}, current={is_current}')
        return is_current, 'not modified' if is_current else 'modified'
    except (ValueError, TypeError) as e:
        logger.warning(f'Error parsing dates: {e}')
        return False, f'date parse error: {e}'


def _check_hash(
    local_path: str | Path,
    remote_headers: dict,
    local_metadata: dict,
) -> tuple[bool, str]:

    remote_md5 = remote_headers.get('Content-MD5') or remote_headers.get('content-md5')

    if remote_md5:
        local_md5 = _compute_hash(local_path, 'md5')
        is_current = remote_md5 == local_md5
        logger.debug(f'MD5 check: remote={remote_md5}, local={local_md5}, current={is_current}')
        return is_current, 'md5 match' if is_current else 'md5 mismatch'

    local_sha256 = local_metadata.get('sha256')
    if local_sha256:
        return False, 'hash check requires download'

    return False, 'method_unavailable'


def _check_size(local_path: str | Path, remote_headers: dict) -> tuple[bool, str]:

    remote_size = remote_headers.get('Content-Length') or remote_headers.get('content-length')

    if not remote_size:
        return False, 'method_unavailable'

    try:
        remote_size = int(remote_size)
        local_size = os.path.getsize(local_path)
        is_current = local_size == remote_size
        logger.debug(f'Size check: remote={remote_size}, local={local_size}, current={is_current}')
        return is_current, 'size match' if is_current else 'size mismatch'
    except (ValueError, OSError) as e:
        logger.warning(f'Error checking size: {e}')
        return False, f'size check error: {e}'


def _compute_hash(file_path: str | Path, algorithm: str = 'sha256') -> str:

    h = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_remote_headers(url: str, **kwargs) -> dict:

    import requests

    try:
        response = requests.head(url, allow_redirects=True, **kwargs)
        logger.debug(f'HEAD request to {url}: status={response.status_code}')
        return dict(response.headers)
    except Exception as e:
        logger.warning(f'Error getting remote headers: {e}')
        return {}
