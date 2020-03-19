from typing import NewType

from sqlalchemy import Column, Integer, Sequence, String

from ImageSaverLib4.MetaDB.Types import ColumnPrinterMixin
from . import RSBase

StorageID = NewType('StorageID', int)
StorageIdentifier = NewType('StorageIdentifier', str)


class ManagedStorage(RSBase, ColumnPrinterMixin):
    __tablename__ = 'managedstorages'
    storage_id = Column(Integer, Sequence('storage_id_seq'), primary_key=True, unique=True)  # type: StorageID
    storage_ident = Column(String(255), unique=True)  # type: StorageIdentifier

    def __init__(self, storage_ident):
        # type: (StorageIdentifier) -> None
        self.storage_ident = storage_ident
