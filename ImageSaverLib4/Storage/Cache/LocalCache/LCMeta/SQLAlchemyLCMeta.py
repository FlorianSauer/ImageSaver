from sqlalchemy.orm import sessionmaker, Session

from ImageSaverLib4.MetaDB.Errors import NotExistingException, AlreadyExistsException
from ImageSaverLib4.MetaDB.SQLAlchemyHelperMixin2 import SQLAlchemyHelperMixin, ExposableGeneratorQuery
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
            with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
                assert len(self._get_all2(exposed_session, ResourceAlias, ResourceAlias.alias_id,
                                          ResourceAlias.resource_name == resource_name)) == 0
            with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
                assert len(self._get_all2(exposed_session, ResourceAlias, ResourceAlias.alias_id,
                                          ResourceAlias.resource_name_alias == alias)) == 1
            with self.session_scope() as session:  # type: Session
                self._create_or_update(session, ResourceAlias, [ResourceAlias.resource_name_alias == alias],
                                       {ResourceAlias.resource_name: resource_name,
                                        ResourceAlias.resource_hash: resource_hash})

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

    # M = TypeVar('M')
    #
    # def _get_or_create(self, session, model, init_args=None, **search_kwargs):
    #     # type: (Session, Type[M], Optional[Dict[str, Any]], **Any) -> M
    #     """
    #
    #     :param model: class to create or get
    #     :param init_args: the values the instance should have if new generated
    #     :param search_kwargs: search values
    #     :return:
    #     """
    #     # traceback.print_exc()
    #     # print(model, init_args, search_kwargs)
    #     with self.session_scope():
    #         instance = self.session.query(model).filter_by(**search_kwargs).first()
    #     if instance:
    #         # self.session.expunge(instance)
    #         return instance
    #     else:
    #         params = dict((k, v) for k, v in search_kwargs.items() if not isinstance(v, ClauseElement))
    #         params.update(init_args or {})
    #         instance = model(**params)
    #         with self.session_scope():
    #             # self.session.begin_nested()
    #             try:
    #                 self.session.add(instance)
    #                 self.session.commit()
    #                 # self.session.expunge(instance)
    #             except IntegrityError as e:
    #                 # self.session.rollback()
    #                 # print("!!!ROLLBACK!!! _get_or_create")
    #                 raise AlreadyExistsException(
    #                     "cannot create new model " + model.__name__ + " by " + repr(params) + '; ' + repr(e))
    #             return instance
    #
    # def _delete(self, session, model, *args):
    #     # type: (Session, Type[M], *BinaryExpression) -> None
    #     # self.session.begin_nested()
    #     try:
    #         session.query(model).filter(*args).delete(synchronize_session='fetch')
    #         session.flush()
    #         session.commit()
    #     except IntegrityError:
    #         # self.session.rollback()
    #         # print("!!!ROLLBACK!!! _delete")
    #
    #         raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))
