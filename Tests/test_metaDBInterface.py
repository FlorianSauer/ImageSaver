import os
from configparser import ConfigParser
from unittest import TestCase

from ImageSaverLib4.MetaDB.MetaBuilder import MetaBuilder
from ImageSaverLib4.MetaDB.db_inits import sqliteRAM, SqliteFileBuilder, SqliteRamBuilder, PostgresBuilder


def toAbsPath(s):
    return
class TestMetaDBInterface(TestCase):
    def makeDemoDB(self, echo=False):
        return sqliteRAM(echo=echo, recreate=True)

    def makeRLDB(self, echo=False):
        CONF_NAME = '.isl_config.conf'
        CONF_PATH = '~/' + CONF_NAME
        path = os.path.abspath(os.path.normpath(os.path.expanduser(CONF_PATH)))
        with open(path, 'r') as f:
            parser = ConfigParser()
            parser.read_file(f)

            meta_builder = MetaBuilder()
            meta_builder.addMetaClass(SqliteRamBuilder)
            meta_builder.addMetaClass(SqliteFileBuilder)
            meta_builder.addMetaClass(PostgresBuilder)
            meta = meta_builder.build_from_config(parser, force_debug=echo)
        return meta

    def test_getAllCompounds(self):
        meta = self.makeRLDB(echo=True)
        comp_len = len(meta.getAllCompounds())
        comp_name_len = len(meta.getAllCompoundNames())
        print(comp_len, comp_name_len)
        self.assertEqual(comp_len, comp_name_len)
        i = 0
        for compound in meta.getAllCompounds(None, False):
            if compound.compound_name == 'demo.vc':
                print(compound)
                i += 1
            if compound.compound_name == 'demo2.vc':
                print(compound)
                i += 1
        self.assertEqual(2, i)

    def test_getUnreferencedFragments(self):
        meta = self.makeRLDB(True)
        list(meta.getUnreferencedFragments())

    def test_getMultipleUsedCompoundsCount(self):
        meta = self.makeRLDB(echo=True)
        meta.getMultipleUsedCompoundsCount()