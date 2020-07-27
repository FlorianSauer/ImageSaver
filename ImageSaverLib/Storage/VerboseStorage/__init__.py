import sys
from typing import cast, Callable, TextIO

import humanfriendly

from ImageSaverLib.MetaDB.Types.Resource import ResourceSize, ResourceName, ResourceHash
from ImageSaverLib.Storage.StorageInterface import StorageInterface, SizableStorageInterface


class VerboseStorage(StorageInterface):
    def __init__(self, storage, verbose=False, out_pipe=sys.stderr):
        # type: (StorageInterface, bool, TextIO) -> None
        super().__init__()

        self._out_pipe = out_pipe

        self._storage = storage
        self._verbose = verbose
        self._on_loadRessource = lambda r_n, r_s: print('Loading Resource', r_n,
                                                        '(' + humanfriendly.format_size(r_s) + ')',
                                                        file=self._out_pipe
                                                        )  # type: Callable[[ResourceName, ResourceSize], None]
        self._on_saveResource = lambda r_h, r_s: print('Saving Resource', '(' + humanfriendly.format_size(r_s) + ')',
                                                  file=self._out_pipe
                                                  )  # type: Callable[[ResourceHash, ResourceSize], None]
        self._on_deleteResource = lambda r_n: print('Deleting Resource', r_n,
                                                    file=self._out_pipe
                                                    )  # type: Callable[[ResourceName], None]
        self._on_listResourceNames = lambda: print('Listing Resource Names',
                                                    file=self._out_pipe
                                                    )  # type: Callable[[], None]
        self._on_wipeResources = lambda: print('Wiping Resources',
                                                    file=self._out_pipe
                                                    )  # type: Callable[[], None]

    @property
    def verbose(self):
        # type: () -> bool
        return self._verbose

    @verbose.setter
    def verbose(self, verbose):
        # type: (bool) -> None
        self._verbose = verbose

    @property
    def on_loadRessource(self):
        # type: () -> Callable[[ResourceName, ResourceSize], None]
        return self._on_loadRessource

    @on_loadRessource.setter
    def on_loadRessource(self, callback):
        # type: (Callable[[ResourceName, ResourceSize], None]) -> None
        self._on_loadRessource = callback

    @property
    def on_saveResource(self):
        # type: () -> Callable[[ResourceHash, ResourceSize], None]
        return self._on_saveResource

    @on_saveResource.setter
    def on_saveResource(self, callback):
        # type: (Callable[[ResourceSize], None]) -> None
        self._on_saveResource = callback

    @property
    def on_deleteResource(self):
        # type: () -> Callable[[ResourceName], None]
        return self._on_deleteResource

    @on_deleteResource.setter
    def on_deleteResource(self, callback):
        # type: (Callable[[ResourceName], None]) -> None
        self._on_deleteResource = callback

    @property
    def on_listResourceNames(self):
        # type: () -> Callable[[], None]
        return self._on_listResourceNames

    @on_listResourceNames.setter
    def on_listResourceNames(self, callback):
        # type: (Callable[[], None]) -> None
        self._on_listResourceNames = callback
    @property
    def on_wipeResources(self):
        # type: () -> Callable[[], None]
        return self._on_wipeResources

    @on_wipeResources.setter
    def on_wipeResources(self, callback):
        # type: (Callable[[], None]) -> None
        self._on_wipeResources = callback

    def getMaxSupportedResourceSize(self):
        return self._storage.getMaxSupportedResourceSize()

    def getRequiredWrapType(self):
        return self._storage.getRequiredWrapType()

    def supportsWrapType(self, wrap_type):
        return self._storage.supportsWrapType(wrap_type)

    def identifier(self):
        return self._storage.identifier()

    def loadRessource(self, resource_name):
        resource_data = self._storage.loadRessource(resource_name)
        if self.verbose:
            self.on_loadRessource(resource_name, cast(ResourceSize, len(resource_data)))
        return resource_data

    def saveResource(self, resource_data, resource_hash, resource_size):
        if self.verbose:
            self.on_saveResource(resource_hash, resource_size)
        return self._storage.saveResource(resource_data, resource_hash, resource_size)

    def deleteResource(self, resource_name):
        if self.verbose:
            self.on_deleteResource(resource_name)
        return self._storage.deleteResource(resource_name)

    def listResourceNames(self):
        if self.verbose:
            self.on_listResourceNames()
        return self._storage.listResourceNames()

    def wipeResources(self):
        if self.verbose:
            self.on_wipeResources()
        return self._storage.wipeResources()


class SizableVerboseStorage(SizableStorageInterface, VerboseStorage):

    def __init__(self, storage, verbose=False, out_pipe=sys.stderr):
        # type: (SizableStorageInterface, bool, TextIO) -> None
        VerboseStorage.__init__(self, storage, verbose, out_pipe)
        self._storage = cast(SizableStorageInterface, self._storage)

    def getTotalSize(self):
        return self._storage.getTotalSize()

    def getCurrentSize(self):
        return self._storage.getCurrentSize()

    def increaseCurrentSize(self, size):
        return self._storage.increaseCurrentSize(size)

    def resetCurrentSize(self):
        return self._storage.resetCurrentSize()

    def calculateFullness(self, default_total_size=None):
        return self._storage.calculateFullness(default_total_size)

    def hasFreeSize(self, required_space):
        return self._storage.hasFreeSize(required_space)
