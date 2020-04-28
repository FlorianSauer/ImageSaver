import traceback
from contextlib import contextmanager
from sqlite3 import Connection as SQLite3Connection
from threading import RLock
from typing import TypeVar, Generic, Type, List, Optional, Dict, Any, Generator

from sqlalchemy import engine, event
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import sessionmaker, scoped_session, Session, Query
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy_utils import database_exists, create_database

from ImageSaverLib.Helpers.SizedGenerator import SizedGenerator
from ImageSaverLib.MetaDB.Errors import AlreadyExistsException, NotExistingException

M = TypeVar('M')
ARGS = TypeVar('ARGS')
KWARGS = TypeVar('KWARGS', )
KWARGS_DICT = Dict[str, KWARGS]


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def init_db(engine, base, recreate=False):
    # type: (engine, DeclarativeMeta, bool) -> SQLAlchemyHelperMixin
    if not database_exists(engine.url):
        create_database(engine.url)
    db_sessionmaker = scoped_session(sessionmaker(bind=engine, expire_on_commit=False, autocommit=False))
    base.query = db_sessionmaker.query_property()
    if recreate:
        base.metadata.drop_all(bind=engine)
    base.metadata.create_all(engine)
    return SQLAlchemyHelperMixin(db_sessionmaker)


class SQLAlchemyHelperMixin(Generic[M]):

    def __init__(self, session):
        # type: (sessionmaker) -> None
        self.sessionmaker = session
        # noinspection PyTypeChecker
        self.session = None  # type: Session
        self._session_lock = RLock()
        self._context = None
        self.yield_size = 10000
        self._closed = False

    # region Context Methods

    def __enter__(self):
        with self._session_lock:
            if self._closed:
                raise RuntimeError('DB Sessions closed.')
            assert self.session is None
            self._context = self.session_scope()
            self._context.__enter__()
            assert self.session is not None
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._session_lock:
            if self._closed:
                raise RuntimeError('DB Sessions closed.')
            assert self.session is not None
            self._context.__exit__(exc_type, exc_val, exc_tb)
            assert self.session is None
            self._context = None
            self.close()

    @contextmanager
    def session_scope(self):
        with self._session_lock:
            if self.session:
                # self._session_lock.acquire()
                yield self.session
                # print("expunging all")
                self.session.expunge_all()
            else:
                # self._session_lock.acquire()
                session = self.sessionmaker()  # type: Session
                self.session = session
                try:
                    # self.session.begin_nested()
                    yield session
                    session.flush()
                    session.commit()
                    # session.commit()
                except Exception:
                    session.rollback()
                    raise
                finally:
                    # noinspection PyTypeChecker
                    self.session = None  # type: Session
                    # print("finally expunging all")
                    # for obj in session:
                    #     session.expunge(obj)
                    #     assert obj not in session
                    session.expunge_all()
                    # print("closing")
                    session.close()
    # @contextmanager
    # def session_scope(self):
    #     if self.session:
    #         yield self.session
    #     else:
    #
    #         session = self.sessionmaker()  # type: Session
    #         self.session = session
    #         session.begin_nested()
    #         try:
    #             yield session
    #             session.flush()
    #             session.commit()
    #         except Exception:
    #             session.rollback()
    #             raise
    #         finally:
    #             # noinspection PyTypeChecker
    #             self.session = None  # type: Session
    #             session.expunge_all()
    #             session.close()

    # endregion

    def close(self):
        with self.session_scope():
            # noinspection PyUnresolvedReferences
            self.session.close_all_sessions()
            self._closed = True

    # region db helpers
    def _get_or_create(self, model, init_args=None, **search_kwargs):
        # type: (Type[M], Optional[Dict[str, Any]], **Any) -> M
        """

        :param model: class to create or get
        :param init_args: the values the instance should have if new generated
        :param search_kwargs: search values
        :return:
        """
        # traceback.print_exc()
        # print(model, init_args, search_kwargs)
        instance = self.session.query(model).filter_by(**search_kwargs).first()
        if instance:
            # self.session.expunge(instance)
            self.session.expunge_all()
            return instance
        else:
            params = dict((k, v) for k, v in search_kwargs.items() if not isinstance(v, ClauseElement))
            params.update(init_args or {})
            instance = model(**params)
            self.session.begin_nested()
            try:
                self.session.add(instance)
                self.session.commit()
                # self.session.expunge(instance)
            except IntegrityError:
                self.session.rollback()
                # print("!!!ROLLBACK!!! _get_or_create")
                raise AlreadyExistsException("cannot create new model " + model.__name__ + " by " + repr(params))
            self.session.expunge_all()
            return instance

    def _create_or_update(self, model, get_by, update_to, **kwargs):
        # type: (Type[M], List[BinaryExpression], Dict[InstrumentedAttribute, Any], **Any) -> M
        """

        :param model: class to create or get
        :param get_by: get by these filter expressions
        :param update_to: update row with these
        :param kwargs: the values the instance should have if new generated
        :return:
        """
        query = self.session.query(model).filter(*get_by)  # type: Query
        instance = query.first()
        if instance:
            self.session.begin_nested()
            try:
                query.update(update_to)
                self.session.commit()
            except IntegrityError:
                print("ROLLBACK" * 100)
                traceback.print_exc()
                self.session.rollback()
                raise
            instance = query.first()
            return instance
        else:
            params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
            instance = model(**params)
            self.session.begin_nested()
            try:
                self.session.add(instance)
                self.session.commit()
            except IntegrityError:
                print("ROLLBACK" * 100)
                traceback.print_exc()
                self.session.rollback()
                raise
                # raise AlreadyExistsException("cannot create new model " + model.__name__ + " by " + repr(params))
            return instance

    def _create(self, model, **kwargs):
        # type: (Type[M], **Any) -> M
        """

        :param model: class to create or get
        :param kwargs: search values
        :return:
        """
        # traceback.print_exc()
        # print(model, kwargs)
        instance = model(**kwargs)
        self.session.begin_nested()
        try:
            self.session.add(instance)
            self.session.flush()
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            # print("!!!ROLLBACK!!! _create")
            raise AlreadyExistsException("cannot create new model " + model.__name__ + " by " + repr(kwargs))
        return instance

    def _get(self, model, *args):
        # type: (Type[M], *BinaryExpression) -> M
        instance = self.session.query(model).filter(*args).first()
        if instance:
            self.session.expunge_all()
            return instance
        else:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    # Todo: replace filter_by with filter+binaryexpressions
    def _get_all(self, model, order_by, **kwargs):
        # type: (Type[M], InstrumentedAttribute, **Any) -> SizedGenerator[M]
        query = self.session.query(model).filter_by(**kwargs).order_by(order_by)  # type: Query
        count = query.count()
        instances = self.__query_yielder(query, self.yield_size)
        lengen = SizedGenerator(instances, count)
        if count > 0:
            return lengen
        elif count == 0:
            return lengen
        else:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(kwargs))

    def _get_all2(self, model, order_by, *args):
        # type: (Type[M], InstrumentedAttribute, *BinaryExpression) -> SizedGenerator[M]
        query = self.session.query(model).filter(*args).order_by(order_by)  # type: Query
        count = query.count()
        instances = self.__query_yielder(query, self.yield_size)
        lengen = SizedGenerator(instances, count)
        if count > 0:
            return lengen
        elif count == 0:
            return lengen
        else:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    def _lengen_query(self, query, order_by=None):
        # type: (Query, Optional[InstrumentedAttribute]) -> SizedGenerator
        """
        Given query MUST specify a primary key in oder to fetch all rows. Some DB-APIs will return a random set, which
        must be ordered to return it in a generator.
        """
        if order_by:
            query = query.order_by(order_by)
        count = query.count
        gen = self.__query_yielder(query, self.yield_size)
        return SizedGenerator(gen, count)

    def __query_yielder(self, query, batch_size):
        # type: (Query[M], int) -> Generator[M, Any, None]
        offset = 0
        while True:
            r = False
            for elem in query.limit(batch_size).offset(offset):
                r = True
                # query.session.expunge(elem)
                query.session.expunge_all()
                yield elem
            offset += batch_size
            if not r:
                break

    def _put(self, instance):
        self.session.begin_nested()
        try:
            self.session.add(instance)
            self.session.flush()
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            # print("!!!ROLLBACK!!! _put")
            raise

    def _delete(self, model, *args):
        # type: (Type[M], *BinaryExpression) -> None
        self.session.begin_nested()
        try:
            self.session.query(model).filter(*args).delete(synchronize_session='fetch')
            self.session.flush()
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            # print("!!!ROLLBACK!!! _delete")

            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    def _update(self, model, get_by, update_to):
        # type: (Type[M], List[BinaryExpression], Dict[InstrumentedAttribute, Any]) -> M
        """

        :param model: class to create or get
        :param get_by: get by these filter expressions
        :param update_to: update row with these
        :return:
        """
        query = self.session.query(model).filter(*get_by)  # type: Query
        try:
            instance = query.one()
        except NoResultFound:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(get_by))
        if instance:
            self.session.begin_nested()
            try:
                query.update(update_to)
                self.session.commit()
            except IntegrityError:
                print("ROLLBACK" * 100)
                traceback.print_exc()
                self.session.rollback()
                raise
            instance = query.first()
            return instance
        else:
            raise NotExistingException

    def _get_one(self, model, *args):
        # type: (Type[M], *BinaryExpression) -> M
        try:
            instance = self.session.query(model).filter(*args).one()
            # self.session.expunge(instance)
            return instance
        except NoResultFound:
            pass
        raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    # endregion
