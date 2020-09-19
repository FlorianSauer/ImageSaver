from sqlalchemy.orm import sessionmaker, Session

from ImageSaverLib.MetaDB.Errors import NotExistingException, AlreadyExistsException
from ImageSaverLib.MetaDB.SQLAlchemyHelperMixin2 import SQLAlchemyHelperMixin, ExposableGeneratorQuery
from .LCMetaInterface import LCMetaInterface
from .ResourceAlias import ResourceAlias


class SQLAlchemyLCMeta(LCMetaInterface, SQLAlchemyHelperMixin):

    def __init__(self, session):
        # type: (sessionmaker) -> None
        super().__init__()
        SQLAlchemyHelperMixin.__init__(self, session)
        LCMetaInterface.__init__(self)

    def close(self):
        return SQLAlchemyHelperMixin.close(self)

    def addAlias(self, resource_name, alias, resource_hash):
        try:
            with self.session_scope() as session:  # type: Session
                self._get_or_create(session, ResourceAlias, init_args=None, resource_name=resource_name,
                                    resource_name_alias=alias, resource_hash=resource_hash)
        except AlreadyExistsException:
            with self.session_scope() as session:  # type: Session
                try:
                    self._delete(session, ResourceAlias, ResourceAlias.resource_name == resource_name)
                except NotExistingException:
                    pass
                self._create_or_update(session, ResourceAlias, [ResourceAlias.resource_name_alias == alias],
                                       {ResourceAlias.resource_name: resource_name,
                                        # ResourceAlias.resource_name_alias: alias,
                                        ResourceAlias.resource_hash: resource_hash},
                                       **dict(resource_name=resource_name,
                                              resource_name_alias=alias,
                                              resource_hash=resource_hash)
                                       )

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

    def getResourceHashForAlias(self, alias):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, ResourceAlias, ResourceAlias.resource_name_alias == alias)
