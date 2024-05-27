import os


class Cache:

    def __init__(self, path: str):

        self._set_path(path)


    def _set_path(path: str):

        if os.path.isdir(path):

            path = os.path.join(path, 'cache.sqlite')

        self.path = path
