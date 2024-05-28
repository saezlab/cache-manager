import functools as _ft

from pypath_common import log as _log, session as _session

session = _ft.partial(_session, 'cache_manager')
log = _ft.partial(_log, 'cache_manager')
