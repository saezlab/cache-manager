"""
Cache management for directories of files.
"""

from ._metadata import __version__, __author__, __license__
from ._metadata import metadata as _metadata
from ._session import log, _log, session
from ._cache import Cache
from . import _data as data
