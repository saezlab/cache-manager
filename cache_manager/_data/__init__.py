import functools as ft
from pkg_infra import data

load = ft.partial(data.load, module="cache_manager")
