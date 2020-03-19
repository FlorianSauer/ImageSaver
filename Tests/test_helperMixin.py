import os
from unittest import TestCase

from sqlalchemy import Sequence, Integer, Column, LargeBinary, BigInteger
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from ImageSaverLib4.MetaDB.SQLAlchemyHelperMixin import SQLAlchemyHelperMixin
from ImageSaverLib4.MetaDB.Types import ColumnPrinterMixin

Base = declarative_base()


class Model(Base, ColumnPrinterMixin):
    __tablename__ = 'models'
    fragment_id = Column(Integer, Sequence('model_id_seq'), primary_key=True, unique=True)  # type: int
    fragment_hash = Column(LargeBinary(64), unique=True, index=True)  # type: bytes
    fragment_size = Column(BigInteger)  # type: int
    fragment_payload_size = Column(BigInteger)  # type: int

    def __init__(self, fragment_hash, fragment_size, fragment_payload_size):
        # type: (bytes, int, int) -> None
        self.fragment_hash = fragment_hash
        self.fragment_size = fragment_size
        self.fragment_payload_size = fragment_payload_size


class DB(SQLAlchemyHelperMixin):
    def createModel(self, b, s1, s2):
        with self.session_scope():
            self._create(Model, fragment_hash=b, fragment_size=s1, fragment_payload_size=s2)

    def getModelS1(self, s1):
        with self.session_scope():
            return self._get_one(Model, Model.fragment_size == s1)

    def getModelS2(self, s2):
        with self.session_scope():
            return self._get_one(Model, Model.fragment_payload_size == s2)


def sqliteFile(filepath, echo=False, recreate=False):
    # type: (str, bool, bool) -> DB
    return init_db(create_engine('sqlite:///' + filepath, echo=echo, connect_args={'check_same_thread': False},
                                 # poolclass=StaticPool
                                 ), recreate=recreate)


def init_db(engine, recreate=False):
    # type: (Engine, bool) -> DB
    db_session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))
    Base.query = db_session.query_property()
    if recreate:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(engine)
    return DB(db_session)


class TestFragmentCache(TestCase):
    TEST_ITERATIONS = 250000

    def test_addFragment(self):
        db = sqliteFile('R:/tmp2.sqlite', recreate=True)
        with db:
            input('start')
            for index in range(self.TEST_ITERATIONS):
                db.createModel(os.urandom(32), index, index)
            input('query s1')
            for index in range(self.TEST_ITERATIONS):
                model = db.getModelS1(index)
                self.assertEqual(index, model.fragment_payload_size)
                self.assertEqual(index, model.fragment_size)
            input('query s2')
            for index in range(self.TEST_ITERATIONS):
                model = db.getModelS2(index)
                self.assertEqual(index, model.fragment_payload_size)
                self.assertEqual(index, model.fragment_size)
                # if index / self.TEST_ITERATIONS >= 0.2 or index >= 50000:
                #     input('break')

            input('done')
