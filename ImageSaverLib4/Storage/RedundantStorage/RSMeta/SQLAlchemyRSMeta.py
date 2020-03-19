from sqlalchemy.orm import sessionmaker, Session

from ImageSaverLib4.MetaDB.Errors import NotExistingException, AlreadyExistsException
from ImageSaverLib4.MetaDB.SQLAlchemyHelperMixin2 import SQLAlchemyHelperMixin, ExposableGeneratorQuery
from ImageSaverLib4.Storage.RedundantStorage.RSMeta.ManagedStorage import ManagedStorage
from .RSMetaInterface import RSMetaInterface
from .ResourceAlias import ResourceAlias


class SQLAlchemyRSMeta(RSMetaInterface, SQLAlchemyHelperMixin):

    def __init__(self, session):
        # type: (sessionmaker) -> None
        super().__init__()
        SQLAlchemyHelperMixin.__init__(self, session)
        RSMetaInterface.__init__(self)

    def close(self):
        return SQLAlchemyHelperMixin.close(self)

    def addAlias(self, resource_name, alias):
        with self.session_scope() as session:  # type: Session
            self._get_or_create(session, ResourceAlias, init_args=None, resource_name=resource_name,
                                resource_name_alias=alias)

    def renameAlias(self, resource_name, alias):
        with self.session_scope() as session:  # type: Session
            self._update(session, ResourceAlias,
                         [ResourceAlias.resource_name == resource_name],
                         {ResourceAlias.resource_name_alias: alias})

    def getAliasOfResourceName(self, resource_name):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, ResourceAlias,
                                 ResourceAlias.resource_name == resource_name).resource_name_alias

    def hasAliasForResourceName(self, resource_name):
        with self.session_scope() as session:  # type: Session
            try:
                self._get_one(session, ResourceAlias, ResourceAlias.resource_name == resource_name)
                return True
            except NotExistingException:
                return False

    def removeAliasOfResourceName(self, resource_name):
        with self.session_scope() as session:  # type: Session
            self._delete(session, ResourceAlias, ResourceAlias.resource_name == resource_name)
        assert not self.hasAliasForResourceName(resource_name)

    def getAllResourceNames(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            len_gen = self._get_all2(exposed_session, ResourceAlias, ResourceAlias.alias_id)
            return len_gen.add_layer(lambda gen: (ra.resource_name for ra in gen))

    def getAllResourceNamesWithAliases(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            len_gen = self._get_all2(exposed_session, ResourceAlias, ResourceAlias.alias_id)
            return len_gen.add_layer(lambda gen: ((ra.resource_name, ra.resource_name_alias) for ra in gen))

    def makeManagedStorage(self, storage_ident):
        with self.session_scope() as session:
            return self._get_or_create(session, ManagedStorage, None, storage_ident=storage_ident)

    def makeMultipleManagedStorages(self, storage_ident_list):
        r_dict = {}
        with self.session_scope() as session:
            for storage_ident in storage_ident_list:
                r_dict[storage_ident] = self._get_or_create(session, ManagedStorage, None, storage_ident=storage_ident)
        return r_dict

    def listManagedStorages(self):
        with self.exposable_session_scope() as exposed_session:
            return self._get_all2(exposed_session, ManagedStorage, ManagedStorage.storage_id)

    def hasManagedStorage(self, storage_ident):
        with self.session_scope() as session:  # type: Session
            try:
                self._get_one(session, ManagedStorage, ManagedStorage.storage_ident == storage_ident)
                return True
            except NotExistingException:
                return False


