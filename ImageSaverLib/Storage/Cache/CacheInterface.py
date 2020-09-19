from abc import ABC, abstractmethod


class CacheInterface(ABC):
    def __init__(self, wrapped_storage):
        self.cache_enabled = True
        self.wrapped_storage = wrapped_storage
