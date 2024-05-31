"""
Cache management for directories of files.
"""

from ._metadata import __version__, __author__, __license__
from ._metadata import metadata as _metadata
from . import _session

session = _session.session()
log = _session.log
_log = session._logger.msg