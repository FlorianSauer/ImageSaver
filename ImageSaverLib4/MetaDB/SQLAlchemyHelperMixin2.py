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

from ImageSaverLib4.Helpers.SizedGenerator import SizedGenerator
from ImageSaverLib4.MetaDB.Errors import AlreadyExistsException, NotExistingException

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


class ExposableGeneratorQuery(object):
    def __init__(self, session):
        # type: (Session) -> None
        self.session = session
        self._closed = False

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.failureClosing()

    def failureClosing(self):
        if not self._closed:
            try:
                self.session.flush()
                self.session.commit()
            except Exception:
                self.session.rollback()
                raise
            finally:
                self._closed = True
                self.session.expunge_all()
                self.session.close()


class SQLAlchemyHelperMixin(Generic[M]):

    def __init__(self, session):
        # type: (sessionmaker) -> None
        self.sessionmaker = session
        # noinspection PyTypeChecker
        self._session_lock = RLock()
        self.yield_size = 10000
        self._closed = False

    # region Context Methods

    def __enter__(self):
        with self._session_lock:
            if self._closed:
                raise RuntimeError('DB Sessions closed.')
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._session_lock:
            if self._closed:
                raise RuntimeError('DB Sessions closed.')
            self.close()

    @contextmanager
    def session_scope(self):
        # type: () -> Session
        with self._session_lock:
            # self._session_lock.acquire()
            session = self.sessionmaker()  # type: Session
            # noinspection PyBroadException
            try:
                # self.session.begin_nested()
                yield session
                session.flush()
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                # noinspection PyTypeChecker
                session.expunge_all()
                session.close()
                # self.sessionmaker.

    @contextmanager
    def exposable_session_scope(self):
        # type: () -> ExposableGeneratorQuery
        with self._session_lock:
            session = self.sessionmaker()  # type: Session
            exposable_session = ExposableGeneratorQuery(session)
            # noinspection PyBroadException
            try:
                yield exposable_session
            except Exception:
                exposable_session.failureClosing()
                raise

    # endregion

    def close(self):
        with self.session_scope() as session:  # type: Session
            # noinspection PyUnresolvedReferences
            session.close_all_sessions()
            self._closed = True

    # region db helpers
    def _get_or_create(self, session, model, init_args=None, **search_kwargs):
        # type: (Session, Type[M], Optional[Dict[str, Any]], **Any) -> M
        """

        :param model: class to create or get
        :param init_args: the values the instance should have if new generated
        :param search_kwargs: search values
        :return:
        """
        # traceback.print_exc()
        # print(model, init_args, search_kwargs)
        instance = session.query(model).filter_by(**search_kwargs).first()
        if instance:
            # self.session.expunge(instance)
            session.expunge_all()
            return instance
        else:
            params = dict((k, v) for k, v in search_kwargs.items() if not isinstance(v, ClauseElement))
            params.update(init_args or {})
            instance = model(**params)
            session.begin_nested()
            try:
                session.add(instance)
                session.commit()
                # self.session.expunge(instance)
            except IntegrityError:
                session.rollback()
                # print("!!!ROLLBACK!!! _get_or_create")
                raise AlreadyExistsException("cannot create new model " + model.__name__ + " by " + repr(params))
            session.expunge_all()
            return instance

    def _create_or_update(self, session, model, get_by, update_to, **kwargs):
        # type: (Session, Type[M], List[BinaryExpression], Dict[InstrumentedAttribute, Any], **Any) -> M
        """

        :param model: class to create or get
        :param get_by: get by these filter expressions
        :param update_to: update row with these
        :param kwargs: the values the instance should have if new generated
        :return:
        """
        query = session.query(model).filter(*get_by)  # type: Query
        instance = query.first()
        if instance:
            session.begin_nested()
            try:
                query.update(update_to)
                session.commit()
            except IntegrityError:
                print("ROLLBACK" * 100)
                traceback.print_exc()
                session.rollback()
                raise
            instance = query.first()
            return instance
        else:
            params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
            instance = model(**params)
            session.begin_nested()
            try:
                session.add(instance)
                session.commit()
            except IntegrityError:
                print("ROLLBACK" * 100)
                traceback.print_exc()
                session.rollback()
                raise
                # raise AlreadyExistsException("cannot create new model " + model.__name__ + " by " + repr(params))
            return instance

    def _create(self, session, model, **kwargs):
        # type: (Session, Type[M], **Any) -> M
        """

        :param model: class to create or get
        :param kwargs: search values
        :return:
        """
        # traceback.print_exc()
        # print(model, kwargs)
        instance = model(**kwargs)
        session.begin_nested()
        try:
            session.add(instance)
            session.flush()
            session.commit()
        except IntegrityError:
            session.rollback()
            # print("!!!ROLLBACK!!! _create")
            raise AlreadyExistsException("cannot create new model " + model.__name__ + " by " + repr(kwargs))
        return instance

    def _get(self, session, model, *args):
        # type: (Session, Type[M], *BinaryExpression) -> M
        instance = session.query(model).filter(*args).first()
        if instance:
            session.expunge_all()
            return instance
        else:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    # Todo: replace filter_by with filter+binaryexpressions
    def _get_all(self, exposable_session, model, order_by, **kwargs):
        # type: (ExposableGeneratorQuery, Type[M], InstrumentedAttribute, **Any) -> SizedGenerator[M]
        query = exposable_session.session.query(model).filter_by(**kwargs).order_by(order_by)  # type: Query
        count = query.count()
        instances = self.__exposable_query_yielder(exposable_session, query, self.yield_size)
        lengen = SizedGenerator(instances, count)
        if count > 0:
            return lengen
        elif count == 0:
            return lengen
        else:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(kwargs))

    def _get_all2(self, exposable_session, model, order_by, *filter_args):
        # type: (ExposableGeneratorQuery, Type[M], InstrumentedAttribute, *BinaryExpression) -> SizedGenerator[M]
        query = exposable_session.session.query(model).filter(*filter_args).order_by(order_by)  # type: Query
        count = query.count()
        instances = self.__exposable_query_yielder(exposable_session, query, self.yield_size)
        lengen = SizedGenerator(instances, count)
        if count > 0:
            return lengen
        elif count == 0:
            return lengen
        else:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(filter_args))

    def _exposable_lengen_query(self, exposable_session, query, order_by=None):
        # type: (ExposableGeneratorQuery, Query, Optional[InstrumentedAttribute]) -> SizedGenerator
        """
        Given query MUST specify a primary key in oder to fetch all rows. Some DB-APIs will return a random set, which
        must be ordered to return it in a generator.
        """
        if order_by:
            query = query.order_by(order_by)
        count = query.count
        gen = self.__exposable_query_yielder(exposable_session, query, self.yield_size)
        return SizedGenerator(gen, count)

    def _non_exposable_lengen_query(self, query, order_by=None):
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

    def __exposable_query_yielder(self, exposable_session, query, batch_size):
        # type: (ExposableGeneratorQuery, Query[M], int) -> Generator[M, Any, None]
        with exposable_session:
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

    def _put(self, session, instance):
        # type: (Session, M) -> None
        session.begin_nested()
        try:
            session.add(instance)
            session.flush()
            session.commit()
        except IntegrityError:
            session.rollback()
            # print("!!!ROLLBACK!!! _put")
            raise

    def _delete(self, session, model, *args):
        # type: (Session, Type[M], *BinaryExpression) -> None
        session.begin_nested()
        try:
            session.query(model).filter(*args).delete(synchronize_session='fetch')
            session.flush()
            session.commit()
        except IntegrityError:
            session.rollback()
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    def _update(self, session, model, get_by, update_to):
        # type: (Session, Type[M], List[BinaryExpression], Dict[InstrumentedAttribute, Any]) -> M
        """

        :param model: class to create or get
        :param get_by: get by these filter expressions
        :param update_to: update row with these
        :return:
        """
        query = session.query(model).filter(*get_by)  # type: Query
        try:
            instance = query.one()
        except NoResultFound:
            raise NotExistingException("Not Found " + model.__name__ + " by " + repr(get_by))
        if instance:
            session.begin_nested()
            try:
                query.update(update_to)
                session.commit()
            except IntegrityError:
                print("ROLLBACK" * 100)
                traceback.print_exc()
                session.rollback()
                raise
            instance = query.first()
            return instance
        else:
            raise NotExistingException

    def _get_one(self, session, model, *args):
        # type: (Session, Type[M], *BinaryExpression) -> M
        try:
            instance = session.query(model).filter(*args).one()
            # self.session.expunge(instance)
            return instance
        except NoResultFound:
            pass
        raise NotExistingException("Not Found " + model.__name__ + " by " + repr(args))

    # endregion
