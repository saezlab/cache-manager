"""
Cache management for directories of files.
"""

from . import _data as data
from ._cache import Cache
from ._session import log, _log, session
from ._metadata import metadata as _metadata
from ._metadata import __author__, __license__, __version__
