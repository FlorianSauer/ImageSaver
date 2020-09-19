from typing import Type, Tuple, List, Optional

from sqlalchemy import func, asc, and_
# noinspection PyProtectedMember
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Query, aliased, Session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import not_

from ImageSaverLib.Helpers import chunkiterable_gen
from ImageSaverLib.MetaDB import Base
from ImageSaverLib.MetaDB.Errors import NotExistingException
from ImageSaverLib.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib.MetaDB.SQLAlchemyHelperMixin2 import SQLAlchemyHelperMixin, ExposableGeneratorQuery
from ImageSaverLib.MetaDB.Types.Compound import Compound, CompoundVersion
from ImageSaverLib.MetaDB.Types.CompoundFragmentMapping import CompoundFragmentMapping, SequenceIndex
from ImageSaverLib.MetaDB.Types.Fragment import Fragment
from ImageSaverLib.MetaDB.Types.FragmentResourceMapping import FragmentResourceMapping, FragmentOffset
from ImageSaverLib.MetaDB.Types.Resource import Resource


def init_db(engine, recreate=False):
    # type: (Engine, bool) -> SQLAlchemyMetaDB
    db_session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))
    Base.query = db_session.query_property()
    if recreate:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(engine)
    return SQLAlchemyMetaDB(db_session)


class SQLAlchemyMetaDB(MetaDBInterface, SQLAlchemyHelperMixin):

    def __init__(self, session):
        # type: (scoped_session) -> None
        SQLAlchemyHelperMixin.__init__(self, session)
        MetaDBInterface.__init__(self)

    def close(self):
        pass

    def getCompoundByName(self, compound_name, compound_version=CompoundVersion(None)):
        with self.session_scope() as session:  # type: Session
            return self._get(session, Compound,
                             Compound.compound_name == compound_name,
                             Compound.compound_version == compound_version)

    def getCompoundByHash(self, compound_hash, compound_version=CompoundVersion(None)):
        with self.session_scope() as session:  # type: Session
            return self._get(session, Compound,
                             Compound.compound_hash == compound_hash,
                             Compound.compound_version == compound_version)

    def makeFragment(self, fragment_hash, fragment_size, fragment_payload_size):
        with self.session_scope() as session:  # type: Session
            return self._get_or_create(session, Fragment, None, fragment_hash=fragment_hash,
                                       fragment_size=fragment_size,
                                       fragment_payload_size=fragment_payload_size)

    def hasFragmentByPayloadHash(self, fragment_hash):
        with self.session_scope() as session:  # type: Session
            try:
                self._get(session, Fragment, Fragment.fragment_hash == fragment_hash)
                return True
            except NotExistingException:
                return False

    def getFragmentByPayloadHash(self, fragment_hash):
        with self.session_scope() as session:  # type: Session
            return self._get(session, Fragment, Fragment.fragment_hash == fragment_hash)

    def makeResource(self, resource_name, resource_size, resource_payloadsize, resource_hash, wrap_type, compress_type):
        with self.session_scope() as session:  # type: Session
            # try to create this exact resource
            return self._get_or_create(session, Resource, None, resource_name=resource_name,
                                       resource_size=resource_size,
                                       resource_payloadsize=resource_payloadsize,
                                       resource_hash=resource_hash, wrapping_type=wrap_type,
                                       compression_type=compress_type)

    def setFragmentsMappingForCompound(self, compound_id, fragment_id_sequence_index):
        with self.session_scope() as session:  # type: Session
            self._delete(session, CompoundFragmentMapping, CompoundFragmentMapping.compound_id == compound_id)
            session.bulk_insert_mappings(CompoundFragmentMapping,
                                         (dict(compound_id=compound_id,
                                               fragment_id=fragment_id,
                                               sequence_index=sequence_index)
                                          for fragment_id, sequence_index in fragment_id_sequence_index))
        # with self.session_scope():
        #     try:
        #         for fragment_id, sequence_index in fragment_id_sequence_index:
        #             o = self._get(CompoundFragmentMapping,
        #                              CompoundFragmentMapping.fragment_id==fragment_id,
        #                              CompoundFragmentMapping.sequence_index==sequence_index)
        #             assert o.fragment_id == fragment_id
        #             assert o.sequence_index == sequence_index
        #             assert o.compound_id == compound_id
        #     except Exception as e:
        #         traceback.print_exc()
        #         exit(1)

    def makeCompound(self, name, compound_type, compound_hash, compound_size, wrapping_type, compression_type):
        with self.session_scope() as session:  # type: Session
            return self._get_or_create(session, Compound, None, compound_name=name, compound_type=compound_type,
                                       compound_hash=compound_hash, compound_size=compound_size,
                                       wrapping_type=wrapping_type, compression_type=compression_type,
                                       compound_version=CompoundVersion(None))

    def makeSnapshottedCompound(self, compound):
        with self.session_scope() as session:  # type: Session
            max_version = self._getMaxVersionOfCompound(compound.compound_name, session)
            snapshot_version = 1 if max_version is None else max_version + 1
            # print(snapshot_version)
            return self._create(session, Compound,
                                compound_name=compound.compound_name,
                                compound_type=compound.compound_type,
                                compound_hash=compound.compound_hash,
                                compound_size=compound.compound_size,
                                wrapping_type=compound.wrapping_type,
                                compression_type=compound.compression_type,
                                compound_version=CompoundVersion(snapshot_version))

    def _getMaxVersionOfCompound(self, compound_name, session):
        # type: (str, Session) -> Optional[int]
        query = session.query(func.max(Compound.compound_version))
        query = query.filter(Compound.compound_name == compound_name)
        try:
            return query.one()[0]
        except NoResultFound:
            return None

    def updateCompound(self, name, compound_type, compound_hash, compound_size, wrapping_type, compression_type):
        with self.session_scope() as session:  # type: Session
            return self._update(session, Compound, get_by=[Compound.compound_name == name],
                                update_to={Compound.compound_type: compound_type,
                                           Compound.compound_hash: compound_hash,
                                           Compound.compound_size: compound_size,
                                           Compound.wrapping_type: wrapping_type,
                                           Compound.compression_type: compression_type})

    def hasCompoundWithName(self, name, version=CompoundVersion(None)):
        with self.session_scope() as session:  # type: Session
            try:
                self._get(session, Compound,
                          Compound.compound_name == name,
                          Compound.compound_version == version)
                return True
            except NotExistingException:
                return False

    def hasCompoundWithHash(self, compound_hash, compound_version=CompoundVersion(None)):
        with self.session_scope() as session:  # type: Session
            try:
                self._get(session, Compound,
                          Compound.compound_hash == compound_hash,
                          Compound.compound_version == compound_version)
                return True
            except NotExistingException:
                return False

    def getSequenceIndexSortedFragmentsForCompound(self, compound_id):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            # narrow down Compound-Fragment mappings to only the selected compound
            CompoundFragmentMappingSQ = aliased(CompoundFragmentMapping,
                                                session.query(CompoundFragmentMapping).filter(
                                                    CompoundFragmentMapping.compound_id == compound_id).subquery())  # type: Type[CompoundFragmentMapping]

            query = session.query(CompoundFragmentMappingSQ.sequence_index, Fragment)  # type: Query
            query = query.select_from(CompoundFragmentMappingSQ)
            # join fragments on CompoundFragmentMapping subquery
            query = query.join(Fragment, Fragment.fragment_id == CompoundFragmentMappingSQ.fragment_id)
            # # join Fragment-Resource mapping on Fragments
            # query = query.join(FragmentResourceMapping, FragmentResourceMapping.fragment_id == Fragment.fragment_id)
            # # join Resource on Fragment-Resource mapping
            # query = query.join(Resource, Resource.resource_id == FragmentResourceMapping.resource_id)
            # finally order all by sequence index (ascending, so caller can use it in a for loop to reassemble the file)
            query = query.order_by(CompoundFragmentMappingSQ.sequence_index.asc())

            # query = self.session.query(CompoundFragmentMapping.sequence_index, Fragment, Resource)  # type: Query
            # query = query.select_from(CompoundFragmentMapping, Fragment, Resource)  # type: Query
            # query = query.filter(CompoundFragmentMapping.compound_id == compound_id,
            #                      CompoundFragmentMapping.fragment_id == Fragment.fragment_id,
            #                      Fragment.fragment_id == FragmentResourceMapping.fragment_id,
            #                      FragmentResourceMapping.resource_id == Resource.resource_id)
            # query = query.order_by(CompoundFragmentMapping.sequence_index.asc())
            # for i in query.all():
            #     print(i)
            return self._exposable_lengen_query(exposed_session, query)

    def getFragmentHashesNeededForCompound(self, compound_id):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Fragment.fragment_hash)  # type: Query
            query = query.select_from(CompoundFragmentMapping, Fragment)  # type: Query
            query = query.filter(CompoundFragmentMapping.compound_id == compound_id,
                                 CompoundFragmentMapping.fragment_id == Fragment.fragment_id)
            query = query.order_by(CompoundFragmentMapping.sequence_index.asc())
            len_gen = self._exposable_lengen_query(exposed_session, query)
            return len_gen.add_layer(lambda gen: (h for h, in gen))
            # return SizedGenerator((fh for fh in gen), len(gen))
            # return [fh for fh, in query.all()]

    def getAllCompounds(self, type_filter=None, order_alphabetically=False, starting_with=None, ending_with=None,
                        slash_count=None, min_size=None, include_snapshots=False):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Compound)  # type: Query

            if order_alphabetically:
                # query = query.order_by(asc(collate(Compound.compound_name, 'NOCASE')))
                query = query.order_by(asc(func.lower(Compound.compound_name)))
            else:
                query = query.order_by(Compound.compound_id)
            if type_filter:
                if isinstance(type_filter, str):
                    query = query.filter(Compound.compound_type == type_filter)
                else:
                    query = query.filter(Compound.compound_type in type_filter)
            if starting_with:
                query = query.filter(Compound.compound_name.startswith(starting_with))
            if ending_with:
                query = query.filter(Compound.compound_name.endswith(ending_with))
            if slash_count is not None:
                if slash_count < 0:
                    raise ValueError("only 0 or positive numbers allowed")
                query = query.filter(Compound.compound_name.ilike('/%' * slash_count))
                slash_count += 1
                query = query.filter(not_(Compound.compound_name.ilike('/%' * slash_count)))
            if min_size is not None:
                if min_size < 0:
                    raise ValueError('negative minimum file size')
                query = query.filter(Compound.compound_size >= min_size)
            if not include_snapshots:
                query = query.filter(Compound.compound_version.is_(None))
            else:
                query = query.order_by(asc(Compound.compound_version))
            return self._exposable_lengen_query(exposed_session, query)

    def getAllCompoundsSizeSum(self, type_filter=None, starting_with=None, ending_with=None, slash_count=None,
                               min_size=None):
        with self.session_scope() as session:  # type: Session
            query = session.query(functions.sum(Compound.compound_size))  # type: Query
            if type_filter:
                query = query.filter(Compound.compound_type == type_filter)
            if starting_with:
                query = query.filter(Compound.compound_name.startswith(starting_with))
            if ending_with:
                query = query.filter(Compound.compound_name.endswith(ending_with))
            if slash_count is not None:
                if slash_count < 0:
                    raise ValueError("only 0 or positive numbers allowed")
                query = query.filter(Compound.compound_name.ilike('/%' * slash_count))
                slash_count += 1
                query = query.filter(not_(Compound.compound_name.ilike('/%' * slash_count)))
            if min_size is not None:
                if min_size < 0:
                    raise ValueError('negative minimum file size')
                query = query.filter(Compound.compound_size >= min_size)
            query = query.filter(Compound.compound_version.is_(None))
            size = query.one()[0]
            return 0 if size is None else int(size)

    def getAllCompoundNames(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            len_gen = self._get_all2(exposed_session, Compound, Compound.compound_id,
                                     Compound.compound_version.is_(None))
            return len_gen.add_layer(lambda gen: (c.compound_name for c in gen))

    def getAllCompoundNamesWithVersion(self, include_snapshots=False):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            if include_snapshots:
                len_gen = self._get_all2(exposed_session, Compound, Compound.compound_id)
            else:
                len_gen = self._get_all2(exposed_session, Compound, Compound.compound_id,
                                         Compound.compound_version.is_(None))
            return len_gen.add_layer(lambda gen: ((c.compound_name, c.compound_version) for c in gen))

    def getTotalCompoundSize(self):
        with self.session_scope() as session:  # type: Session
            query = session.query(functions.sum(Compound.compound_size))
            query = query.filter(Compound.compound_version.is_(None))
            size = query.one()[0]
            return 0 if size is None else int(size)

    def getTotalCompoundCount(self, with_type=None):
        with self.session_scope() as session:  # type: Session
            query = session.query(Compound)  # type: Query
            query = query.filter(Compound.compound_version.is_(None))
            if with_type:
                query = query.filter(Compound.compound_type == with_type)  # type: Query
            return query.count()

    def getSnapshotCount(self, with_type=None):
        with self.session_scope() as session:  # type: Session
            query = session.query(Compound)  # type: Query
            query = query.filter(Compound.compound_version.isnot(None))
            if with_type:
                query = query.filter(Compound.compound_type == with_type)  # type: Query
            return query.count()

    def getUniqueCompoundSize(self):
        with self.session_scope() as session:  # type: Session

            # subquery = self.session.query(Compound).distinct(Compound.compound_hash).subquery()
            # subquery = aliased(Compound, subquery)  # type: Compound
            #
            # query = self.session.query(functions.sum(Compound.compound_size))  # type: Query
            # query = query.select_entity_from(subquery)

            exclude_version_sq = session.query(Compound.compound_hash,
                                               Compound.compound_size)  # type: Query
            exclude_version_sq = exclude_version_sq.filter(Compound.compound_version.is_(None))
            exclude_version_sq = exclude_version_sq.subquery()

            subquery = session.query(exclude_version_sq.c.compound_hash.label('compound_hash'),
                                     exclude_version_sq.c.compound_size.label('compound_size'),
                                     # functions.count(Compound.compound_hash)
                                     )  # type: Query
            subquery = subquery.select_from(exclude_version_sq)

            subquery = subquery.group_by(exclude_version_sq.c.compound_hash, exclude_version_sq.c.compound_size)
            subquery = subquery.subquery()

            query = session.query(functions.sum(subquery.c.compound_size))
            query = query.select_from(subquery)

            size = query.one()[0]
            return 0 if size is None else int(size)

    def getUniqueCompoundCount(self):
        with self.session_scope() as session:  # type: Session
            exclude_version_sq = session.query(Compound.compound_hash)  # type: Query
            exclude_version_sq = exclude_version_sq.filter(Compound.compound_version.is_(None))
            exclude_version_sq = exclude_version_sq.subquery()

            query = session.query(exclude_version_sq.c.compound_hash)  # type: Query
            query = query.select_from(exclude_version_sq)
            query = query.group_by(exclude_version_sq.c.compound_hash)
            return query.count()

    def getTotalFragmentSize(self):
        with self.session_scope() as session:  # type: Session
            size = session.query(functions.sum(Fragment.fragment_size)).one()[0]
            return 0 if size is None else int(size)

    def getTotalFragmentCount(self):
        with self.session_scope() as session:  # type: Session
            return session.query(Fragment).count()

    def getTotalResourceSize(self):
        with self.session_scope() as session:  # type: Session
            # sql_size = list(self.session.execute("SELECT sum(fragments.fragment_payload_size) from fragments"))[0][0]
            # size = self.session.query(functions.sum(Fragment.fragment_payload_size)).one()[0]
            # print(sql_size)
            # print(size)
            # sql_size = list(self.session.execute("SELECT sum(resources.resource_size) from resources"))[0][0]
            size = session.query(functions.sum(Resource.resource_size)).one()[0]
            # print(sql_size)
            # print(size)
            # assert sql_size == size, str(sql_size)+' vs '+str(size)
            return 0 if size is None else int(size)

    def getTotalResourceCount(self):
        with self.session_scope() as session:  # type: Session
            return session.query(Resource).count()

    def removeCompound(self, compound_id):
        with self.session_scope() as session:  # type: Session
            self._delete(session, Compound, Compound.compound_id == compound_id)

    def renameCompound(self, old_name, new_name):
        with self.session_scope() as session:  # type: Session
            query = session.query(Compound)  # type: Query
            query = query.filter(Compound.compound_name == old_name)  # type: Query
            count = query.count()
            if count < 1:
                raise NotExistingException('no compound found with name ' + old_name)
            query.update({Compound.compound_name: new_name}, synchronize_session='fetch')

    def renameResource(self, old_resource_name, new_resource_name):
        with self.session_scope() as session:
            query = session.query(Resource)  # type: Query
            query = query.filter(Resource.resource_name == old_resource_name)
            count = query.count()
            if count < 1:
                raise NotExistingException('no resource found with name ' + old_resource_name)
            query.update({Resource.resource_name: new_resource_name}, synchronize_session='fetch')

    def massRenameResource(self, old_new_resource_name_pairs, skip_unknown=False):
        with self.session_scope() as session:
            for old_resource_name, new_resource_name in old_new_resource_name_pairs:
                query = session.query(Resource)  # type: Query
                query = query.filter(Resource.resource_name == old_resource_name)
                count = query.count()
                if count < 1:
                    if skip_unknown:
                        continue
                    raise NotExistingException('no resource found with name ' + old_resource_name)
                query.update({Resource.resource_name: new_resource_name}, synchronize_session='fetch')

    def collectGarbage(self, keep_fragments=False, keep_resources=True):
        with self.session_scope() as session:  # type: Session
            # if not keep_payloads:
            #     query = self.session.query(Payload.payload_id)  # type: Query
            #     query = query.outerjoin(Compound, Payload.payload_id == Compound.payload_id)  # type: Query
            #     query = query.filter(Compound.compound_id.is_(None))
            #     subquery = query.subquery()
            #
            #     query = self.session.query(Payload)  # type: Query
            #     query = query.filter(Payload.payload_id.in_(subquery))
            #     query.delete(synchronize_session='fetch')
            if not keep_fragments:
                query = session.query(Fragment.fragment_id)  # type: Query
                query = query.outerjoin(CompoundFragmentMapping,
                                        Fragment.fragment_id == CompoundFragmentMapping.fragment_id)  # type: Query
                query = query.filter(CompoundFragmentMapping.compound_id.is_(None))
                subquery = query.subquery()

                query = session.query(Fragment)  # type: Query
                query = query.filter(Fragment.fragment_id.in_(subquery))
                # for x in query.all():
                #     print("deleting fragment in cache_meta", x)
                query.delete(synchronize_session='fetch')
            if not keep_resources:
                query = session.query(Resource)  # type: Query
                query = query.filter(
                    not_(Resource.resource_id.in_(session.query(FragmentResourceMapping.resource_id).subquery())))
                query.delete(synchronize_session='fetch')

    # def getPayloadByID(self, payload_id):
    #     with self.session_scope():
    #         return self._get(Payload, Payload.payload_id == payload_id)

    def getUnneededFragments(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Fragment)  # type: Query
            query = query.outerjoin(CompoundFragmentMapping,
                                    Fragment.fragment_id == CompoundFragmentMapping.fragment_id)  # type: Query
            query = query.filter(CompoundFragmentMapping.compound_id.is_(None))
            query = query.distinct()
            query = query.order_by(Fragment.fragment_id)
            return self._exposable_lengen_query(exposed_session, query)

    def getUnneededFragmentHashes(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Fragment.fragment_hash)  # type: Query
            query = query.outerjoin(CompoundFragmentMapping,
                                    Fragment.fragment_id == CompoundFragmentMapping.fragment_id)  # type: Query
            query = query.filter(CompoundFragmentMapping.compound_id.is_(None))
            query = query.order_by(Fragment.fragment_id)
            lengen = self._exposable_lengen_query(exposed_session, query)
            return lengen.add_layer(lambda gen: (fht[0] for fht in gen))
            # return SizedGenerator.layer_adder(lengen, lambda gen: (fht[0] for fht in gen))
            # return SizedGenerator((fht[0] for fht in lengen), len(lengen))
            # return [fht[0] for fht in query.all()]

    def deleteResourceByID(self, resource_id):
        with self.session_scope() as session:  # type: Session
            self._delete(session, Resource, Resource.resource_id == resource_id)

    def deleteResourceByName(self, resource_name):
        with self.session_scope() as session:  # type: Session
            self._delete(session, Resource, Resource.resource_name == resource_name)

    def getResourceForFragment(self, fragment_id):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, Resource, Resource.resource_id == FragmentResourceMapping.resource_id,
                                 FragmentResourceMapping.fragment_id == fragment_id)

    def truncateAllCompounds(self):
        with self.session_scope() as session:  # type: Session
            self._delete(session, Compound)

    def getDuplicateFragmentsCount(self):
        with self.session_scope() as session:  # type: Session
            sub_query = session.query(CompoundFragmentMapping.fragment_id)  # type: Query
            sub_query = sub_query.select_from(Compound)
            sub_query = sub_query.join(CompoundFragmentMapping,
                                       Compound.compound_id == CompoundFragmentMapping.compound_id)
            sub_query = sub_query.filter(Compound.compound_version.is_(None))
            sub_query = sub_query.subquery()

            query = session.query(CompoundFragmentMapping.fragment_id,
                                  func.count(CompoundFragmentMapping.fragment_id))
            query = query.select_entity_from(sub_query)
            query = query.group_by(CompoundFragmentMapping.fragment_id)  # type: Query
            query = query.having(func.count(CompoundFragmentMapping.fragment_id) > 1)
            return query.count()

    def getSavedBytesByDuplicateFragments(self):
        with self.session_scope() as session:  # type: Session
            snapshot_excluded_sq = session.query(CompoundFragmentMapping.fragment_id)  # type: Query
            snapshot_excluded_sq = snapshot_excluded_sq.select_from(Compound)
            snapshot_excluded_sq = snapshot_excluded_sq.join(CompoundFragmentMapping,
                                                             Compound.compound_id == CompoundFragmentMapping.compound_id)
            snapshot_excluded_sq = snapshot_excluded_sq.filter(Compound.compound_version.is_(None))
            snapshot_excluded_sq = snapshot_excluded_sq.subquery()

            subquery_1 = session.query(CompoundFragmentMapping.fragment_id,
                                       func.count(CompoundFragmentMapping.fragment_id).label('fragment_id_count'))
            subquery_1 = subquery_1.select_entity_from(snapshot_excluded_sq)
            subquery_1 = subquery_1.group_by(CompoundFragmentMapping.fragment_id)  # type: Query
            subquery_1 = subquery_1.having(func.count(CompoundFragmentMapping.fragment_id) > 1)
            subquery_1 = subquery_1.subquery()

            subquery_2 = session.query(Fragment.fragment_size, subquery_1.c.fragment_id_count)  # type: Query
            subquery_2 = subquery_2.select_from(subquery_1)
            subquery_2 = subquery_2.join(Fragment, Fragment.fragment_id == subquery_1.c.fragment_id)
            subquery_2 = subquery_2.order_by(Fragment.fragment_id)

            subquery_2 = subquery_2.filter(subquery_1.c.fragment_id_count > 1)

            subquery_2 = subquery_2.subquery()
            query = session.query(func.sum(subquery_2.c.fragment_size * (subquery_2.c.fragment_id_count - 1)))
            query = query.select_from(subquery_2)
            result = query.one()[0]
            if result is None:
                return 0
            return result

    def getMultipleUsedCompoundsCount(self, compound_type=None):
        with self.session_scope() as session:  # type: Session
            query1 = session.query(Compound.compound_hash,
                                   func.count(Compound.compound_hash).label('compound_hash_count'))
            query1 = query1.filter(Compound.compound_version.is_(None))
            query1 = query1.group_by(Compound.compound_hash)
            query1 = query1.order_by(Compound.compound_hash)
            if compound_type:
                query1 = query1.filter(Compound.compound_type == compound_type)

            subquery = query1.subquery()
            query = session.query(func.count(subquery.c.compound_hash), func.sum(subquery.c.compound_hash_count))
            query = query.select_from(subquery)
            compound_count2, hash_count2 = query.one()
            if hash_count2 is None:
                hash_count2 = 0
            return hash_count2 - compound_count2

    def getSavedBytesByMultipleUsedCompounds(self):
        with self.session_scope() as session:  # type: Session
            # subquery = self.session.query(Compound.compound_hash,
            #                               func.count(Compound.compound_hash).label('compound_hash_count'))
            # subquery = subquery.group_by(Compound.compound_hash)  # type: Query
            # subquery = subquery.having(func.count(Compound.compound_hash) > 1)
            # subquery = subquery.subquery()
            #
            # query = self.session.query(Compound.compound_size, subquery.c.compound_hash_count)  # type: Query
            # query = query.select_from(subquery)
            # query = query.join(Compound, Compound.compound_hash == subquery.c.compound_hash)
            # query = query.group_by(Compound.compound_hash)
            #
            compound_hash_count_label = func.count(Compound.compound_hash).label('compound_hash_count')
            subquery = session.query(Compound.compound_hash, Compound.compound_size,
                                     compound_hash_count_label)  # type: Query
            subquery = subquery.filter(Compound.compound_version.is_(None))
            subquery = subquery.group_by(Compound.compound_hash, Compound.compound_size)
            subquery = subquery.having(func.count(Compound.compound_hash) > 1)
            # s1 = sum(((s * c) - s for h, s, c in self._non_exposable_lengen_query(subquery) if c > 1))

            # subquery = subquery.filter(compound_hash_count_label > 1)
            subquery = subquery.subquery()

            query = session.query(func.sum((subquery.c.compound_size * subquery.c.compound_hash_count)
                                           - subquery.c.compound_size))
            query = query.select_from(subquery)
            query = query.filter(subquery.c.compound_hash_count > 1)

            result = query.one()[0]
            if result is None:
                return 0
            return result
            # assert s1 == s2
            # return s2

    def getAllResourceNames(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Resource.resource_name)  # type: Query
            query = query.distinct()
            lengen = self._exposable_lengen_query(exposed_session, query, Resource.resource_id)
            return lengen.add_layer(lambda gen: (rn[0] for rn in gen))
            # return SizedGenerator.layer_adder(lengen, lambda gen: (rn[0] for rn in gen))
            # return SizedGenerator((rn[0] for rn in lengen), len(lengen))
            # return [rn[0] for rn in query.all()]

    def getAllResources(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            return self._get_all2(exposed_session, Resource, Resource.resource_id)

    def getAllResourcesSizeSorted(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            return self._exposable_lengen_query(exposed_session,
                                                session.query(Resource).order_by(Resource.resource_size.desc(),
                                                                                 Resource.resource_id))

    def getResourceNameForResourceHash(self, resource_hash):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, Resource, Resource.resource_hash == resource_hash).resource_name

    def getFragmentByID(self, fragment_id):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, Fragment, Fragment.fragment_id == fragment_id)

    def getResourceOffsetForFragment(self, fragment_id):
        with self.session_scope() as session:  # type: Session
            try:
                return_list = session.query(Resource, FragmentResourceMapping.fragment_offset).filter(
                    FragmentResourceMapping.fragment_id == fragment_id,
                    FragmentResourceMapping.resource_id == Resource.resource_id).one()
            except NoResultFound:
                raise NotExistingException("No offsets exist for Fragment with id " + repr(fragment_id))
            # for resource, offset in return_list:
            #     self.session.expunge(resource)
            #     self.session.expunge(offset)
            return return_list
            # return self._get_one(FragmentResourceMapping,
            #                      FragmentResourceMapping.fragment_id == fragment_id).fragment_offset

    def getResourceForResourceHash(self, resource_hash):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, Resource, Resource.resource_hash == resource_hash)

    def hasFragmentResourceMappingForFragment(self, fragment_id):
        with self.session_scope() as session:  # type: Session
            res = session.query(FragmentResourceMapping).filter(
                FragmentResourceMapping.fragment_id == fragment_id).count()
            if res == 1:
                return True
            elif res == 0:
                return False
            else:
                raise NotImplementedError

    def makeFragmentResourceMapping(self, fragment_id, resource_id, fragment_offset):
        with self.session_scope() as session:  # type: Session
            self._get_or_create(session, FragmentResourceMapping, fragment_id=fragment_id, resource_id=resource_id,
                                fragment_offset=fragment_offset)

    def makeMultipleFragmentResourceMapping(self, resource_id, fragment_id_fragment_offset):
        with self.session_scope() as session:  # type: Session
            for chunk in chunkiterable_gen((fid for fid, _ in fragment_id_fragment_offset), 500, skip_none=True):
                try:
                    self._delete(session, FragmentResourceMapping, FragmentResourceMapping.fragment_id.in_(chunk))
                except NotExistingException:
                    pass
            session.bulk_insert_mappings(FragmentResourceMapping,
                                         (dict(fragment_id=fragment_id,
                                               resource_id=resource_id,
                                               fragment_offset=fragment_offset)
                                          for fragment_id, fragment_offset in fragment_id_fragment_offset))
            # for obj in self.session:
            #     self.session.expunge(obj)
            # self.session.expunge_all()

    def getSmallestResource(self, ignore=None):
        with self.session_scope() as session:  # type: Session

            referenced_resources = aliased(FragmentResourceMapping, session.query(FragmentResourceMapping).distinct(
                FragmentResourceMapping.resource_id).subquery())  # type: Type[FragmentResourceMapping]

            query = session.query(Resource)
            query = query.select_from(referenced_resources)
            query = query.join(Resource, Resource.resource_id == referenced_resources.resource_id)
            if ignore:
                query = query.filter(Resource.resource_hash.notin_(ignore))
            query = query.order_by(Resource.resource_payloadsize.asc())
            instance = query.first()
            # self.session.expunge(instance)
            return instance

    def updateResource(self, resource_id, resource_name, resource_size, resource_payloadsize, resource_hash,
                       resource_wrap_type,
                       resource_compress_type):
        with self.session_scope() as session:  # type: Session
            old_resource = self._get(session, Resource, Resource.resource_id == resource_id)
            # self.session.expunge(old_resource)
            new_resource = self._update(session,
                                        Resource,
                                        get_by=[Resource.resource_id == resource_id, ],
                                        update_to={Resource.resource_name: resource_name,
                                                   Resource.resource_size: resource_size,
                                                   Resource.resource_payloadsize: resource_payloadsize,
                                                   Resource.resource_hash: resource_hash,
                                                   Resource.wrapping_type: resource_wrap_type,
                                                   Resource.compression_type: resource_compress_type})
            if new_resource.resource_hash != old_resource.resource_hash:
                self._create(session, Resource, resource_name=old_resource.resource_name,
                             resource_size=old_resource.resource_size,
                             resource_payloadsize=old_resource.resource_payloadsize,
                             resource_hash=old_resource.resource_hash, wrapping_type=old_resource.wrapping_type,
                             compression_type=old_resource.compression_type)
            return new_resource

    def getUnreferencedFragments(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Fragment, CompoundFragmentMapping)  # type: Query
            query = query.outerjoin(CompoundFragmentMapping,
                                    CompoundFragmentMapping.fragment_id == Fragment.fragment_id)  # type: Query
            query = query.filter(CompoundFragmentMapping.payload_fragment_id.is_(None))
            # for i in query.all():
            #     print(i)
            query = query.order_by(Fragment.fragment_id)
            lengen = self._exposable_lengen_query(exposed_session, query)
            return lengen.add_layer(lambda gen: (i[0] for i in gen))
            # return SizedGenerator((i[0] for i in lengen), len(lengen))
            # return [i[0] for i in query.all()]

    def deleteFragments(self, unreferenced_fragments):
        fragment_ids = set((f.fragment_id for f in unreferenced_fragments))
        with self.session_scope() as session:  # type: Session
            query = session.query(Fragment)  # type: Query
            # if only_pending:
            #     query = query.filter(Fragment.fragment_pending.is_(True))
            for chunk in chunkiterable_gen(fragment_ids, 500, skip_none=True):
                query.filter(Fragment.fragment_id.in_(chunk)).delete(synchronize_session='fetch')

    def deleteUnreferencedFragments(self):
        with self.session_scope() as session:  # type: Session
            query = session.query(Fragment.fragment_id)  # type: Query
            query = query.outerjoin(CompoundFragmentMapping,
                                    CompoundFragmentMapping.fragment_id == Fragment.fragment_id)  # type: Query
            query = query.filter(CompoundFragmentMapping.payload_fragment_id.is_(None))
            # for i in query.all():
            #     print(i)
            # query = query.order_by(Fragment.fragment_id)
            session.query(Fragment).filter(Fragment.fragment_id.in_(query.subquery())).delete(
                synchronize_session='fetch')

    def getResourceByResourceName(self, resource_name):
        with self.session_scope() as session:  # type: Session
            return self._get_one(session, Resource, Resource.resource_name == resource_name)

    def getUnreferencedResources(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Resource, FragmentResourceMapping)  # type: Query
            query = query.outerjoin(FragmentResourceMapping,
                                    FragmentResourceMapping.resource_id == Resource.resource_id)  # type: Query
            query = query.filter(FragmentResourceMapping.fragment_resource_mapping_id.is_(None))
            # for i in query.all():
            #     print(i)
            query = query.order_by(Resource.resource_id)
            lengen = self._exposable_lengen_query(exposed_session, query)
            return lengen.add_layer(lambda gen: (i[0] for i in gen))
            # return SizedGenerator((i[0] for i in lengen), len(lengen))
            # return [i[0] for i in query.all()]

    def removeCompoundByName(self, compoundname, keep_snapshots=False):
        with self.session_scope() as session:  # type: Session
            if keep_snapshots:
                self._delete(session, Compound,
                             Compound.compound_name == compoundname,
                             Compound.compound_version.is_(None))
            else:
                self._delete(session, Compound, Compound.compound_name == compoundname)

    def getResourceWithReferencedFragmentSize(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Resource, functions.sum(Fragment.fragment_size))  # type: Query
            query = query.select_from(Resource)
            query = query.outerjoin(FragmentResourceMapping,
                                    FragmentResourceMapping.resource_id == Resource.resource_id)
            query = query.join(Fragment, Fragment.fragment_id == FragmentResourceMapping.fragment_id)
            query = query.group_by(Resource.resource_id)
            query = query.order_by(Resource.resource_id)
            lengen = self._exposable_lengen_query(exposed_session, query)
            return lengen.add_layer(lambda gen: ((r, int(s)) for r, s in gen))
            # return SizedGenerator.layer_adder(lengen, lambda gen: ((r, int(s)) for r, s in gen))
            # return SizedGenerator(((r, int(s)) for r, s in lengen), len(lengen))
            # return [(r, int(s)) for r, s in query.all()]

    def getFragmentsWithOffsetOnResource(self, resource_id):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            FragmentResourceMappingSQ = aliased(FragmentResourceMapping, session.query(FragmentResourceMapping).filter(
                FragmentResourceMapping.resource_id == resource_id).subquery())  # type: Type[FragmentResourceMapping]
            query = session.query(Fragment, FragmentResourceMappingSQ.fragment_offset)  # type: Query
            query = query.select_from(FragmentResourceMappingSQ)
            query = query.join(Fragment, Fragment.fragment_id == FragmentResourceMappingSQ.fragment_id)
            return self._exposable_lengen_query(exposed_session, query, Fragment.fragment_id)

    def moveFragmentMappings(self, old_resource, new_resource):
        with self.session_scope() as session:  # type: Session
            query = session.query(FragmentResourceMapping)
            query = query.filter(FragmentResourceMapping.resource_id == old_resource)  # type: Query
            query.update({FragmentResourceMapping.resource_id: new_resource})

    def getAllFragments(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            return self._get_all2(exposed_session, Fragment, Fragment.fragment_id)

    def getAllFragmentsWithNoResourceLink(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Fragment)
            query = query.outerjoin(FragmentResourceMapping,
                                    FragmentResourceMapping.fragment_id == Fragment.fragment_id)
            query = query.filter(FragmentResourceMapping.fragment_resource_mapping_id.is_(None))
            # if not include_pending:
            #     query = query.filter(Fragment.fragment_pending.is_(False))
            return self._exposable_lengen_query(exposed_session, query, Fragment.fragment_id)

    def getAllCompoundsWithNoFragmentLink(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Compound)
            query = query.outerjoin(CompoundFragmentMapping,
                                    CompoundFragmentMapping.compound_id == Compound.compound_id)
            query = query.filter(Compound.compound_size > 0)
            query = query.filter(CompoundFragmentMapping.payload_fragment_id.is_(None))
            return self._exposable_lengen_query(exposed_session, query, Compound.compound_id)

    def getCompoundByHashWithFragmentLinks(self, compound_hash):
        with self.session_scope() as session:  # type: Session
            query = session.query(Compound)  # type: Query
            query = query.filter(Compound.compound_hash == compound_hash)

            query = query.outerjoin(CompoundFragmentMapping,
                                    CompoundFragmentMapping.compound_id == Compound.compound_id)
            query = query.filter(Compound.compound_size > 0)
            query = query.filter(not_(CompoundFragmentMapping.payload_fragment_id.is_(None)))
            c = query.first()
            if not c:
                raise NotExistingException

    def getAllFragmentsSortedByCompoundUsage(self):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            fragment_distinct = session.query(func.min(CompoundFragmentMapping.compound_id).label('compound_id'),
                                              CompoundFragmentMapping.fragment_id)  # type: Query
            fragment_distinct = fragment_distinct.select_from(CompoundFragmentMapping)
            fragment_distinct = fragment_distinct.group_by(CompoundFragmentMapping.fragment_id)
            fragment_distinct = fragment_distinct.subquery('fragment_distinct')

            query = session.query(fragment_distinct.c.compound_id, CompoundFragmentMapping.sequence_index,
                                  Fragment)  # type: Query
            query = query.select_from(fragment_distinct)
            query = query.outerjoin(CompoundFragmentMapping,
                                    and_(fragment_distinct.c.compound_id == CompoundFragmentMapping.compound_id,
                                         fragment_distinct.c.fragment_id == CompoundFragmentMapping.fragment_id))
            query = query.outerjoin(Fragment, fragment_distinct.c.fragment_id == Fragment.fragment_id)
            query = query.order_by(fragment_distinct.c.compound_id.asc(), CompoundFragmentMapping.sequence_index.asc())

            return self._exposable_lengen_query(exposed_session, query)

    def makeAndMapFragmentsToResource(self, resource_id, fragments_offset):
        with self.session_scope() as session:  # type: Session
            new_fragments_offset = []  # type: List[Tuple[Fragment, FragmentOffset]]
            for fragment, offset in fragments_offset:
                new_fragment = self._get_or_create(session, Fragment, None, fragment_hash=fragment.fragment_hash,
                                                   fragment_size=fragment.fragment_size,
                                                   fragment_payload_size=fragment.fragment_payload_size)
                new_fragments_offset.append((new_fragment, offset))
            for chunk in chunkiterable_gen((f.fragment_id for f, _ in new_fragments_offset), 500, skip_none=True):
                try:
                    self._delete(session, FragmentResourceMapping, FragmentResourceMapping.fragment_id.in_(chunk))
                except NotExistingException:
                    pass
            session.bulk_insert_mappings(FragmentResourceMapping,
                                         (dict(fragment_id=fragment.fragment_id,
                                               resource_id=resource_id,
                                               fragment_offset=fragment_offset)
                                          for fragment, fragment_offset in new_fragments_offset))
            return new_fragments_offset

    def addOverwriteCompoundAndMapFragments(self, compound, fragment_payload_index):
        with self.session_scope() as session:  # type: Session
            try:
                self._update(session,
                             Compound,
                             [Compound.compound_name == compound.compound_name,
                              Compound.compound_version == compound.compound_version],
                             {Compound.compound_type: compound.compound_type,
                              Compound.compound_hash: compound.compound_hash,
                              Compound.compound_size: compound.compound_size,
                              Compound.wrapping_type: compound.wrapping_type,
                              Compound.compression_type: compound.compression_type,
                              Compound.compound_version: compound.compound_version})
            except NotExistingException:
                session.add(compound)
                session.flush()
            new_compound = self._get_one(session, Compound,
                                         Compound.compound_name == compound.compound_name,
                                         Compound.compound_version == compound.compound_version)
            new_fragment_payload_index = []  # type: List[Tuple[Fragment, SequenceIndex]]
            for fragment, payload_index in fragment_payload_index:
                new_fragment = self._get_one(session, Fragment, Fragment.fragment_hash == fragment.fragment_hash)
                new_fragment_payload_index.append((new_fragment, payload_index))
            self._delete(session, CompoundFragmentMapping,
                         CompoundFragmentMapping.compound_id == new_compound.compound_id)
            session.bulk_insert_mappings(CompoundFragmentMapping,
                                         (dict(compound_id=new_compound.compound_id,
                                               fragment_id=fragment.fragment_id,
                                               sequence_index=sequence_index)
                                          for fragment, sequence_index in new_fragment_payload_index))

    def getSnapshotsOfCompound(self, compound_name, min_version=None, max_version=None, include_live_version=False):
        with self.exposable_session_scope() as exposed_session:  # type: ExposableGeneratorQuery
            session = exposed_session.session
            query = session.query(Compound)  # type: Query
            query = query.filter(Compound.compound_name == compound_name)
            if not include_live_version:
                query = query.filter(Compound.compound_version.isnot(None))
            if min_version is not None:
                query = query.filter(Compound.compound_version >= min_version)
            if max_version is not None:
                query = query.filter(Compound.compound_version <= max_version)
            query = query.order_by(Compound.compound_version)

            return self._exposable_lengen_query(exposed_session, query)
