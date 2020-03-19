from typing import NewType

from sqlalchemy import Column, Integer, Sequence, String, LargeBinary

from ImageSaverLib4.MetaDB.Types import ColumnPrinterMixin
from ImageSaverLib4.MetaDB.Types.Resource import ResourceName, ResourceHash
from . import LCBase

AliasID = NewType('AliasID', int)
ResourceNameAlias = NewType('ResourceNameAlias', ResourceName)


class ResourceAlias(LCBase, ColumnPrinterMixin):
    __tablename__ = 'resourcealiases'
    alias_id = Column(Integer, Sequence('alias_id_seq'), primary_key=True, unique=True)  # type: AliasID
    resource_name = Column(String(255), unique=True)  # type: ResourceName
    resource_name_alias = Column(String(255), unique=True)  # type: ResourceNameAlias
    resource_hash = Column(LargeBinary(64))  # type: ResourceHash

    def __init__(self, resource_name, resource_name_alias, resource_hash):
        # type: (ResourceName, ResourceNameAlias, ResourceHash) -> None
        self.resource_name = resource_name
        self.resource_name_alias = resource_name_alias
        self.resource_hash = resource_hash
