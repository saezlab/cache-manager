"""
Cache management for directories of files.
"""

from ._metadata import __version__, __author__, __license__
from ._metadata import metadata as _metadata
from . import _session

session = _session._session()
