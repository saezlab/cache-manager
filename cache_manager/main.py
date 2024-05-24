from __future__ import annotations

import os

import utils

class CacheItem:
    """
    Cache item class, stores a single cache item information
    """

    def __init__(
            self,
            key,
            version: int,
            status: int,
            ext: str | None = None,
            label: str | None = None,
            attrs: dict | None = None,
        ):

        self.key = key
        self.version = version
        self.status = status
        self.ext = ext
        self.label = label
        self.attrs = attrs or {}

    @classmethod
    def new(cls, uri, attrs):
        key = cls.serialize(uri, attrs)
        
        return cls(key)
    
    @classmethod
    def serialize(cls, uri, attrs: dict | None = None):
        attrs = attrs or {}
        attrs['uri'] = uri

        return utils.serialize(attrs)
        

    
    def path(self, version: int | None  = None):

        version  = self.default_version if version is None else version

        return f'{self.key}-{version}.{self.ext}'

    