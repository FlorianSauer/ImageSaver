import os

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool

from .LCMetaInterface import LCMetaInterface
from .SQLAlchemyLCMeta import SQLAlchemyLCMeta
from . import LCBase as Base


# noinspection PyUnresolvedReferences
def register_types_on_base():
    from .ResourceAlias import ResourceAlias as _


def init_db(engine, recreate=False):
    # type: (Engine, bool) -> SQLAlchemyLCMeta
    db_session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))
    Base.query = db_session.query_property()
    if recreate:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(engine)
    return SQLAlchemyLCMeta(db_session)


def makeSQLiteMeta(filepath, echo=False):
    # type: (str, bool) -> LCMetaInterface
    register_types_on_base()
    path = os.path.dirname(filepath)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    engine = create_engine('sqlite:///' + filepath, echo=echo, connect_args={'check_same_thread': False},
                           poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def do_connect(dbapi_connection, connection_record):
        # disable pysqlite's emitting of the BEGIN statement entirely.
        # also stops it from emitting COMMIT before any DDL.
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def do_begin(conn):
        # emit our own BEGIN
        conn.execute("BEGIN")
        # try:
        #     conn.execute("BEGIN")
        #     import traceback; traceback.print_exc()
        # except OperationalError:
        #     print('LCMeta', engine, 'already executed BEGIN')
        #     import traceback; traceback.print_exc()
        #     pass
    return init_db(engine, recreate=False)


def makeSQLiteRamMeta(echo=False):
    # type: (bool) -> LCMetaInterface
    from . import LCBase
    register_types_on_base()
    engine = create_engine('sqlite:///:memory:', echo=echo, connect_args={'check_same_thread': False},
                           poolclass=StaticPool)
    db_session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))
    LCBase.query = db_session.query_property()
    LCBase.metadata.create_all(engine)

    @event.listens_for(engine, "connect")
    def do_connect(dbapi_connection, connection_record):
        # disable pysqlite's emitting of the BEGIN statement entirely.
        # also stops it from emitting COMMIT before any DDL.
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def do_begin(conn):
        # emit our own BEGIN
        conn.execute("BEGIN")
        # try:
        #     conn.execute("BEGIN")
        # except OperationalError:
        #     pass
    return SQLAlchemyLCMeta(db_session)

