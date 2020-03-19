from typing import NewType

from sqlalchemy import Column, Integer, Sequence, String, Text

from ImageSaverLib4.MetaDB.Types import ColumnPrinterMixin
from ImageSaverLib4.MetaDB.Types.Resource import ResourceName
from . import RSBase

AliasID = NewType('AliasID', int)
ResourceNameAlias = NewType('ResourceNameAlias', ResourceName)


class ResourceAlias(RSBase, ColumnPrinterMixin):
    __tablename__ = 'resourcealiases'
    alias_id = Column(Integer, Sequence('alias_id_seq'), primary_key=True, unique=True)  # type: AliasID
    resource_name = Column(String(255), unique=True)  # type: ResourceName
    resource_name_alias = Column(Text(), unique=True)  # type: ResourceNameAlias

    def __init__(self, resource_name, resource_name_alias):
        # type: (ResourceName, ResourceNameAlias) -> None
        self.resource_name = resource_name
        self.resource_name_alias = resource_name_alias
