import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from . import Base
from .MetaBuilder import MetaBuilderInterface, str_to_bool
from .MetaDB import MetaDBInterface
from .SQLAlchemyMetaDB import init_db
from .Types import register_types_on_base


def sqliteRAM(echo=False, recreate=False):
    # type: (bool, bool) -> MetaDBInterface
    register_types_on_base()
    engine = create_engine('sqlite:///:memory:', echo=echo, connect_args={'check_same_thread': False},
                           poolclass=StaticPool)

    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def do_connect(dbapi_connection, connection_record):
        # disable pysqlite's emitting of the BEGIN statement entirely.
        # also stops it from emitting COMMIT before any DDL.
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def do_begin(conn):
        # emit our own BEGIN
        conn.execute("BEGIN")

    # # enable autovacuum
    # engine.execute('PRAGMA secure_delete = ON')
    # engine.execute('PRAGMA auto_vacuum = FULL')

    return init_db(engine, recreate=recreate)


def sqliteFile(filepath, echo=False, recreate=False):
    # type: (str, bool, bool) -> MetaDBInterface
    register_types_on_base()
    path = os.path.dirname(filepath)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    engine = create_engine('sqlite:///' + filepath, echo=echo, connect_args={'check_same_thread': False},
                           poolclass=StaticPool)

    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def do_connect(dbapi_connection, connection_record):
        # disable pysqlite's emitting of the BEGIN statement entirely.
        # also stops it from emitting COMMIT before any DDL.
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def do_begin(conn):
        # emit our own BEGIN
        conn.execute("BEGIN")

    # # enable autovacuum
    # engine.execute('PRAGMA secure_delete = ON')
    # engine.execute('PRAGMA auto_vacuum = FULL')

    return init_db(engine, recreate=recreate)


def postgres(username='sqlalchemy', password='sqlalchemy', host='localhost', port=5432, dbname='imagesaver', echo=False,
             recreate=False, timeout=30):
    # type: (str, str, str, int, str, bool, bool, int) -> MetaDBInterface
    register_types_on_base()
    return init_db(
        create_engine('postgresql://' + username + ':' + password + '@' + host + ':' + str(port) + '/' + dbname,
                      echo=echo, connect_args={'connect_timeout': timeout}),
        recreate=recreate)


def copyDB(src_engine, dest_engine, verbose=False, recreate_dest=False):
    # type: (Engine, Engine, bool, bool) -> None
    init_db(dest_engine, recreate=recreate_dest)
    register_types_on_base()
    tables = Base.metadata.tables
    for tbl in tables:
        if verbose:
            print('##################################')
            print(tbl)
            print(tables[tbl].select())
        data = src_engine.execute(tables[tbl].select()).fetchall()
        if verbose:
            for a in data:
                print(a)
        if data:
            if verbose:
                print(tables[tbl].insert())
            dest_engine.execute(tables[tbl].insert(), data)


class SqliteRamBuilder(MetaBuilderInterface):
    __meta_name__ = 'memory'

    @classmethod
    def build(cls, echo='False', recreate='False'):
        echo = str_to_bool(echo)
        recreate = str_to_bool(recreate)
        return sqliteRAM(echo=echo, recreate=recreate)


class SqliteFileBuilder(MetaBuilderInterface):
    __meta_name__ = 'file'

    @classmethod
    def build(cls, path, echo='False', recreate='False'):
        path = os.path.abspath(os.path.normpath(os.path.expanduser(path)))
        echo = str_to_bool(echo)
        recreate = str_to_bool(recreate)
        return sqliteFile(filepath=path, echo=echo, recreate=recreate)


class PostgresBuilder(MetaBuilderInterface):
    __meta_name__ = 'postgres'

    @classmethod
    def build(cls, username, password, host, port='5432', db='imagesaver', echo='False', recreate='False'):
        echo = str_to_bool(echo)
        recreate = str_to_bool(recreate)
        port = int(port)
        return postgres(username, password, host, port, db, echo=echo, recreate=recreate)
