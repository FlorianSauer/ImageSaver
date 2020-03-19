import argparse
import fnmatch
import glob
import hashlib
import os
import re
import stat
import sys
from configparser import ConfigParser
from typing import List, Set, Union, Optional, cast, BinaryIO, IO

import fs
import humanfriendly
from fs.base import FS
from fs.errors import CreateFailed, ResourceNotFound, PermissionDenied, FileExpected
from fs.osfs import OSFS

from ImageSaverLib4.Encapsulation.Wrappers.Types import AES256CTRWrapper, PassThroughWrapper
from ImageSaverLib4.Errors import CompoundNotExistingException
from ImageSaverLib4.FragmentCache import FragmentCache
from ImageSaverLib4.Helpers import get_size_of_stream
from ImageSaverLib4.Helpers.TqdmReporter import TqdmUpTo
from ImageSaverLib4.ImageSaverFS2 import ImageSaverFS
from ImageSaverLib4.ImageSaverLib import ImageSaver
from ImageSaverLib4.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib4.MetaDB.Types.Compound import Compound
from ImageSaverLib4.Storage.StorageInterface import StorageInterface


class Actions(object):
    upload = 'upload'
    download = 'download'
    list = 'list'
    statistic = 'statistic'
    wipe = 'wipe'
    remove = 'remove'
    clean = 'clean'
    check = 'check'
    ftp = 'ftp'


def checkIsPercentage(s):
    if '%' in s:
        s = s.replace('%', '', 1)
    try:
        return float(s) / 100
    except ValueError:
        raise argparse.ArgumentTypeError(repr(s) + " is not a valid Percentage")


def checkIsDirectory(s):
    if s == '-':
        return '-'
    if not os.path.isdir(s):
        raise argparse.ArgumentTypeError(repr(s) + " is not a Directory")
    return s


def checkIsFile(s):
    if not os.path.isfile(s) and s != '*':
        raise argparse.ArgumentTypeError(repr(s) + " is not a File")
    return s


def checkIsPort(s):
    p = int(s)
    if not 0 < p <= 65535:
        raise argparse.ArgumentTypeError(repr(s) + " is not a valid Port (1 to 65535)")
    return p


def checkIsPositive(s):
    i = int(s)
    if i < 0:
        raise argparse.ArgumentTypeError(repr(s) + " is not a positive Integer")
    return i


def sorted_nicely(l):
    """ Sorts the given iterable in the way that is expected.

    Required arguments:
    l -- The iterable to be sorted.

    """
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(l, key=alphanum_key)


def tobytes(a, f):
    # type: (Union[int, float], str) -> int
    return int(humanfriendly.parse_size(str(a) + f))


def fromBytes(b):
    # type: (int) -> str
    return str(humanfriendly.format_size(b, keep_width=True))


def toAbsPath(s):
    return os.path.abspath(os.path.normpath(os.path.expanduser(s)))


class ImageSaverApp(object):
    CONF_NAME = '.isl_config.conf'
    CONF_PATH = '~/' + CONF_NAME

    # region parser setup
    argparser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, allow_abbrev=False)
    argparser.add_argument('--debug', help="Print debug infos", action='store_true')
    argparser.add_argument('--dbecho', help="Print debug infos of DB Connection", action='store_true')
    argparser.add_argument('-s', '--silent', help="Does not print progress bars", action='store_true')
    argparser.add_argument('-v', '--verbose', help="Prints info about uploaded or downloaded Resources", action='store_true')
    argparser.add_argument('-d', '--dryrun', help="Does not save any data in meta db or storage", action='store_true')
    argparser.add_argument('--neutral-dryrun', dest='neutral_dryrun', action='store_true',
                           help='ignores any wrapping settings from the currently used storage, but uses the same '
                                'resource size')
    argparser.add_argument('--dryrun-resourcesize', dest='dryrun_resourcesize',
                           help="sets the resource size which should be used during dryrun",
                           type=humanfriendly.parse_size)
    argparser.add_argument('-nlc', '--no-local-cache', dest='no_local_cache',
                           help="Do not store Resources in a Cache on the local File System.", action='store_true')
    argparser.add_argument('-nrc', '--no-ram-cache', dest='no_ram_cache',
                           help="Do not store Resources in a Cache in RAM. Useful for upload only operations.",
                           action='store_true')
    argparser.add_argument('-c', '--config', help="Path to the Configuration file. (default: %(default)s)", type=toAbsPath,
                           default=CONF_PATH)

    subparsers = argparser.add_subparsers(help='The Action to perform on Target', dest="action")
    upload_parser = subparsers.add_parser(Actions.upload, help="Perform upload operations on Target",
                                          allow_abbrev=False)
    list_parser = subparsers.add_parser(Actions.list, help="Perform list operations on Target", allow_abbrev=False)
    statistic_parser = subparsers.add_parser(Actions.statistic, help="Perform statistic operations on Target",
                                             allow_abbrev=False)
    wipe_parser = subparsers.add_parser(Actions.wipe,
                                        help="Perform wipe operations on Target. "
                                             "Deletes all compounds, but keeps fragments and resources.",
                                        allow_abbrev=False)
    ftp_parser = subparsers.add_parser(Actions.ftp, help="Host a FTP-server on the loopback device",
                                       allow_abbrev=False)
    download_parser = subparsers.add_parser(Actions.download, help="Perform download operations on Target",
                                            allow_abbrev=False)
    remove_parser = subparsers.add_parser(Actions.remove, help="Perform remove operations on Target",
                                          allow_abbrev=False)
    clean_parser = subparsers.add_parser(Actions.clean, help="Perform clean and GC operations on Target",
                                         allow_abbrev=False)
    check_parser = subparsers.add_parser(Actions.check, help="Perform integrity checks on Meta-DB and Target",
                                         allow_abbrev=False)

    upload_parser.add_argument('item', action='append', help="Add the given File or Directory to the Target."
                               , nargs='+', default=[])
    upload_parser.add_argument('-ow', '--overwrite', action='store_true',
                               help="Overwrite Compounds which are already present on the remote side.")
    upload_parser.add_argument('-u', '--update', action='store_true',
                               help="If a Compound is overrideable (Compound already exists and should be overwritten), "
                                    "first generate a hash of the local Compound and compare it to the already saved "
                                    "one. Only if the hashes differ, a Compound gets re-uploaded again. "
                                    "Uploading from stdin ignores this option.")
    upload_parser.add_argument('-r', '--recursive', action='store_true',
                               help="Upload Directories recursively to the Target")
    upload_parser.add_argument('-i', '--stdin', action='store_true',
                               help="Upload data from stdin. The streamed data will be saved with the name of the "
                                    "first given item.")
    upload_parser.add_argument('-fs', '--fragment-size', dest='fragment_size',
                               help="Sets the Fragment Size to the given Value",
                               type=humanfriendly.parse_size)
    upload_parser.add_argument('-c1', '--compress1', choices=['pass', 'zlib', 'lzma', 'bz2'], default='zlib',
                               help="sets the used compressing algorithm during fragment creation. (default: %(default)s)")
    upload_parser.add_argument('-c2', '--compress2', choices=['pass', 'zlib', 'lzma', 'bz2'], default='pass',
                               help="sets the used compressing algorithm during resource creation. (default: %(default)s)")
    upload_parser.add_argument('-fp', '--fragment-policy', choices=['pass', 'fill', 'fill_always'], default='pass',
                               dest='fragment_policy',
                               help="Sets the Fragment upload/flush policy for the fragment cache. "
                                    "'pass' will only build percentage filled resources. Recommended to quickly upload "
                                    "lots of Data."
                                    "'fill' will mainly upload percentage filled resource, but also tries to append "
                                    "fragments to already existing Resources. Recommended to quickly upload lots of "
                                    "Data, but also use Resource space more efficiently. "
                                    "'fill_always' will only append fragments to resources. Only recommended if the "
                                    "main portion of the data is already uploaded and only changes should get saved."
                                    "This policy will create lots of resources during uploading, which should get "
                                    "garbage collected. The remaining Resources however are used efficiently. (default: %(default)s)")
    upload_parser.add_argument('-e', '--exclude', action='append',
                               help="Exclude items that match the given expression.")
    upload_parser.add_argument('-p', '--prefix', default=None, help="sets a path prefix to each uploaded item")
    upload_parser.add_argument('-s', '--sync', action='store_true',
                               help="Mirrors a local Directory to the ImageSaver 'Remote'. Files and Directories which "
                                    "only exists within the ImageSaver domain but not locally get removed.")

    download_parser.add_argument('item', action='append', help="Download the given Item from the Target.",
                                 nargs='+', default=[])
    # download_parser.add_argument('-df', '--download-file', help="Download the given File from the Target",
    #                              dest='file_list', action='append')
    # download_parser.add_argument('-dd', '--download-directory', help="Download the given Directory from the Target",
    #                              dest='directory_list', action='append')
    download_parser.add_argument('-t', '--target', default='.',
                                 help="Save the downloaded Files and Directory into this target directory. "
                                      "To output Compound data through STDOUT, use '-' as target name. (default: %(default)s)",
                                 type=checkIsDirectory)
    download_parser.add_argument('-ow', '--overwrite', action='store_true',
                                 help="Overwrite Files in the target directory with the new downloaded ones")
    download_parser.add_argument('-e', '--exclude', action='append',
                                 help="Exclude items that match the given expression.")
    download_parser.add_argument('-ag', '--advanced-globbing', dest='advanced_globbing', action='store_true',
                                 help="Use a more directory orientated filtering.")
    remove_parser.add_argument('item', action='append', help="Remove the given Item from the Target.",
                               nargs='+', default=[])
    remove_parser.add_argument('-e', '--exclude', action='append',
                               help="Exclude items that match the given expression.")
    remove_parser.add_argument('-ag', '--advanced-globbing', dest='advanced_globbing', action='store_true',
                               help="Use a more directory orientated filtering.")

    wipe_parser.add_argument('-c', '--clean',
                             help="performs a clean operation after wiping. same as using 'clean' afterwards.",
                             action='store_true')

    list_parser.add_argument('filter', nargs='*', default=None, help='Filters the list with the given filter. '
                                                                     'When using a Compound Name and a wildcard query '
                                                                     'at the same time, and the wildcard would also '
                                                                     'cover the given Compound Name, a item can show '
                                                                     'up twice.')
    list_parser.add_argument('-s', '--size', action='store_true', help="Print the Size of a Compound.")
    list_parser.add_argument('-c', '--checksum', action='store_true', help="Print the SHA256 checksum of a Compound.")
    list_parser.add_argument('-f', '--files-only', dest='files_only', action='store_true', help="Only list Files.")
    list_parser.add_argument('-d', '--directories-only', dest='directories_only', action='store_true',
                             help="Only list Directories.")
    list_parser.add_argument('-t', '--tree', action='store_true',
                             help="Print the tree of the uploaded directory structure. Ignores all other options.")
    list_parser.add_argument('-td', '--tree-depth', dest='tree_depth', type=checkIsPositive,
                             help="Limits the tree printing to the given depth.")
    list_parser.add_argument('-ag', '--advanced-globbing', dest='advanced_globbing', action='store_true',
                             help="Use a more directory orientated filtering.")
    statistic_parser.add_argument('-o', '--offline', action='store_true',
                                  help="Only prints Statistics using the database. "
                                       "This does not check the total count of Resources stored in the Storage.")
    clean_parser.add_argument('-os', '--optimize-space',  # action='append',
                              dest='optimize_space',
                              help="Removes 'Fragment-Holes' in Resources; optimizes Space usage of resources by "
                                   "removing all bytes which are not indexed. Optionally a percentage Value can be "
                                   "passed, so only Resources with equals or greater procentual large "
                                   "'Fragment-Holes' get removed.", default=None, const=0.0, nargs='?',
                              type=checkIsPercentage)
    # clean_parser.add_argument('-os', '--optimize-space', action='store_true', dest='optimize_space',
    #                           help="Removes 'Fragment-Holes' in Resources; optimizes Space usage of resources by "
    #                                "removing all bytes which are not indexed.")
    clean_parser.add_argument('-of', '--optimize-fullness', action='store_true', dest='optimize_fullness',
                              help="Combines multiple Fragments across multiple Resources into one Resource if "
                                   "possible. Multiple clean -of calls will mostly rearrange fragments, resulting in "
                                   "lots of Storage Service usage without really improving the overall resource usage. "
                                   "Normally one call is sufficient. This operation will also optimize the space "
                                   "usage, like --optimize-space in a different way.")
    clean_parser.add_argument('-kf', '--keep-fragments', action='store_true', dest='keep_fragments',
                              help="Does not remove Fragments, which are currently not used by any Compound")
    clean_parser.add_argument('-kr', '--keep-resources', action='store_true', dest='keep_resources',
                              help="Does not remove Resources, which are currently not used by any Fragment")
    clean_parser.add_argument('-kur', '--keep-unreferenced-resources', action='store_true',
                              dest='keep_unreferenced_resources',
                              help="Does not remove Resources, which are currently not referenced by Meta")
    clean_parser.add_argument('-df', '--defragment', action='store_true',
                              dest='defragment',
                              help="Defragments the Fragments saved in resources based on the needed order of Compounds."
                              )
    check_parser.add_argument('-cr', '--check-resources', dest='consistency_resourcedata', action='store_true',
                              help="Checks if the Resources saved on Storage yield the correct ResourceHash after "
                                   "Downloading.")
    check_parser.add_argument('-cc', '--check-compounds', dest='consistency_compounddata', action='store_true',
                              help="Checks if the Compounds saved yield the correct Hashes after Downloading.")

    ftp_parser.add_argument('-a', '--address', default='127.0.0.1',
                            help="Sets the listen address of the FTP Server (default: %(default)s)")
    ftp_parser.add_argument('-p', '--port', default=21, type=int,
                            help="Sets the listen port of the FTP Server (default: %(default)s)")
    # endregion

    namespace = argparser.parse_args(sys.argv[1:])

    # print(namespace)
    # exit(1)

    # Todo: add save service by adding a parameter 'service', get subparser and optional add additional
    #  parameters (save dir, dropbox token, ...)

    def __init__(self):
        self._meta = None  # type: Optional[MetaDBInterface]
        self._storage = None  # type: Optional[StorageInterface]
        self._save_service = None  # type: Optional[ImageSaver]
        self._is_fs = None  # type: Optional[ImageSaverFS]
        self._config_parser_obj = None  # type: Optional[ConfigParser]

    @property
    def is_fs(self):
        if self._is_fs:
            return self._is_fs
        else:
            self._is_fs = ImageSaverFS(self.save_service)
            return self._is_fs

    @property
    def save_service(self):
        if self._save_service:
            return self._save_service
        else:
            self._save_service = ImageSaver(self.meta, self.storage,
                                            fragment_size=None,
                                            # fragment_size=tobytes(1, 'MB'),
                                            # resource_size=tobytes(10, 'MB')
                                            )
            parser = self._config_parser()
            if parser.has_option('isl', 'aes_key'):
                self._save_service.wrapper.addWrapper(AES256CTRWrapper(
                    hashlib.sha256(
                        parser.get('isl', 'aes_key').encode('ascii')
                    ).digest()
                ))
                if self._save_service.fragment_cache.resource_wrap_type == PassThroughWrapper.get_wrapper_type():
                    self._save_service.fragment_cache.resource_wrap_type = self._save_service.wrapper.getStackedWrapper(
                        AES256CTRWrapper.get_wrapper_type()).get_wrapper_type()
                else:
                    self._save_service.fragment_cache.resource_wrap_type = self._save_service.wrapper.getStackedWrapper(
                        [AES256CTRWrapper.get_wrapper_type(),
                         self._save_service.fragment_cache.resource_wrap_type]
                    ).get_wrapper_type()
            # self.save_service.fragment_cache = self.cache
            self._save_service.fragment_cache.policy = FragmentCache.POLICY_PASS
            self._save_service.fragment_cache.auto_delete_resource = False
            # self._save_service.wrap_type = self._save_service.wrapper.getStackedWrapper([SizeChecksumWrapper.get_wrapper_type(), AES256CTRWrapper.get_wrapper_type()]).get_wrapper_type()
            # self._save_service.fragment_cache.resource_wrap_type = makeWrappingType(PNGWrapper)
            # self._save_service.fragment_cache.resource_compress_type = makeCompressingType(PassThroughCompressor)
            # self.save_service.changeFragmentSize(tobytes(1, 'MB'))
            # self.save_service.changeFragmentSize((10, 'KB'))
            self._save_service.debug_enabled = self.namespace.debug
            # self._save_service.wrap_type = makeWrappingType(PassThroughWrapper)
            # self._save_service.compress_type = makeCompressingType(ZLibCompressor)  # 31884582
            # if self.namespace.dryrun:
            #     self.namespace.compress1 = 'pass'
            #     self.namespace.compress2 = 'pass'
            #     self.save_service.fragment_cache.resource_wrap_type = PassThroughWrapper.get_wrapper_type()
            #     self.save_service.fragment_cache.resource_compress_type = PassThroughCompressor.get_compressor_type()
            if self.namespace.dryrun:
                self._save_service.fragment_cache.cache_last_downloaded_resource = False
            return self._save_service

    @property
    def storage(self):
        from ImageSaverLib4.Storage.Cache.LocalCache import LocalCache
        from ImageSaverLib4.Storage.Cache.RamCache import RamStorageCache
        from ImageSaverLib4.Storage.DropboxStorage import DropboxStorage
        from ImageSaverLib4.Storage.FileSystemStorage import FileSystemStorage2
        from ImageSaverLib4.Storage.GooglePhotosStorage import GooglePhotosStorage
        from ImageSaverLib4.Storage.RamStorage import RamStorage
        from ImageSaverLib4.Storage.SambaStorage import SambaStorage
        from ImageSaverLib4.Storage.StorageBuilder import StorageBuilder
        from ImageSaverLib4.Storage.SynchronizedStorage import SynchronizedStorage
        from ImageSaverLib4.Storage.VoidStorage import VoidStorage
        from ImageSaverLib4.Storage.RedundantStorage import RedundantStorage
        if self._storage:
            return self._storage
        else:
            storage_builder = StorageBuilder()
            storage_builder.addStorageClass(DropboxStorage)
            storage_builder.addStorageClass(GooglePhotosStorage)
            storage_builder.addStorageClass(FileSystemStorage2)
            storage_builder.addStorageClass(RamStorage)
            storage_builder.addStorageClass(SambaStorage)
            storage_builder.addStorageClass(VoidStorage)
            parser = self._config_parser()
            if parser.has_section('pool'):
                if parser.has_option('pool', 'policy'):
                    policy = parser.get('pool', 'policy').lower()
                    if policy == 'size':
                        policy = RedundantStorage.SIZE
                    elif policy == 'percentage':
                        policy = RedundantStorage.PERCENTAGE
                    else:
                        self.argparser.error(
                            'Config invalid, Section "pool" option "policy" has a invalid value, only "size" or "percentage" is allowed')
                        exit(1)
                        return
                else:
                    policy = RedundantStorage.SIZE
                if parser.has_option('pool', 'redundancy'):
                    try:
                        redundancy = parser.getint('pool', 'redundancy')
                        if redundancy < 1:
                            raise ValueError
                    except ValueError:
                        self.argparser.error(
                            'Config invalid, Section "pool" option "redundancy" is not a positive Integer')
                        exit(1)
                        return
                else:
                    redundancy = 2
                if parser.has_option('pool', 'meta_dir'):
                    meta_dir = parser.get('pool', 'meta_dir')
                else:
                    meta_dir = '~/.isl/.pool'
                storages = storage_builder.build_all_from_config(parser)
                if len(storages) < redundancy:
                    self.argparser.error(
                        'Config invalid, Section "pool" option "redundancy": redundancy too high, not enough storages '
                        'defined')
                    exit(1)
                    return
                storage = RedundantStorage(policy, redundancy, *storages, meta_dir=meta_dir)
            else:
                storage = storage_builder.build_from_config(parser)
            if self.namespace.dryrun:
                self._storage = VoidStorage()
                if not self.namespace.neutral_dryrun:
                    self._storage.required_wrap_type = storage.getRequiredWrapType()
                else:
                    self._storage.required_wrap_type = PassThroughWrapper.get_wrapper_type()
                if self.namespace.dryrun_resourcesize:
                    self._storage.max_resource_size = self.namespace.dryrun_resourcesize
                else:
                    self._storage.max_resource_size = storage.getMaxSupportedResourceSize()
            else:
                if not self.namespace.no_local_cache:
                    if parser.has_option('isl', 'local_cache_size'):
                        try:
                            local_cache_size = parser.getint('isl', 'local_cache_size')
                        except ValueError:
                            self.argparser.error(
                                'Config invalid, Section "isl" option "local_cache_size" is not an Integer')
                            exit(1)
                            return
                    else:
                        local_cache_size = 200
                    storage = LocalCache(self.meta, storage, cache_size=local_cache_size, debug=False)
                if not self.namespace.no_ram_cache:
                    if parser.has_option('isl', 'ram_cache_size'):
                        try:
                            ram_cache_size = parser.getint('isl', 'ram_cache_size')
                        except ValueError:
                            self.argparser.error('Config invalid, Section "isl" option "ram_cache_size" is not an Integer')
                            exit(1)
                            return
                    else:
                        ram_cache_size = 2
                    if ram_cache_size > 0:
                        storage = RamStorageCache(storage, cache_size=ram_cache_size, debug=False)
                self._storage = storage
            self._storage = SynchronizedStorage(self._storage)
            return self._storage

    @property
    def meta(self):
        if self._meta:
            return self._meta
        else:
            from ImageSaverLib4.MetaDB.MetaBuilder import MetaBuilder
            from ImageSaverLib4.MetaDB.db_inits import (sqliteRAM, SqliteRamBuilder, SqliteFileBuilder,
                                                        PostgresBuilder)
            meta_builder = MetaBuilder()
            meta_builder.addMetaClass(SqliteRamBuilder)
            meta_builder.addMetaClass(SqliteFileBuilder)
            meta_builder.addMetaClass(PostgresBuilder)

            meta = meta_builder.build_from_config(self._config_parser(),
                                                  force_debug=self.namespace.dbecho)
            if self.namespace.dryrun:
                self._meta = sqliteRAM(echo=self.namespace.dbecho)
                self.namespace.fragment_policy = 'pass'
            else:
                self._meta = meta
            return self._meta

    def _config_parser(self):
        if not self._config_parser_obj:
            if os.path.exists(self.namespace.config):
                path = self.namespace.config
            elif os.path.exists('./' + self.CONF_NAME):
                path = './' + self.CONF_NAME
            else:
                self.argparser.error('Config is missing!')
                exit(1)
                return

            with open(path, 'r') as f:
                parser = ConfigParser()
                parser.read_file(f)
                self._config_parser_obj = parser
        return self._config_parser_obj

    def setup(self):
        pass
        # self.save_service.login()
        if self.namespace.action is None:
            return
        # if self.namespace.action in (Actions.wipe,):
        #     return
        # self.save_service.checkConsistency()

    def teardown(self):
        # input('done')
        if self.namespace.action is None:
            return
        # self.save_service.logout()

    def run(self):
        # print self.namespace.action
        # if self.namespace.action in (Actions.statistic,):
        #     # self.save_service.collectGarbage()
        #     self.save_service.checkStorageConsistency()
        if self.namespace.silent and self.namespace.verbose:
            self.argparser.error("Cannot run in silent and verbose mode.")
        if self.namespace.action == Actions.upload:
            try:
                self.runUpload()
            except KeyboardInterrupt:
                self.save_service.flush()
                pass
            if self.namespace.dryrun:
                self.namespace.offline = False
                print("")
                print("--- Statistics of dry-run ---")
                print("")
                self.runStatistic()
        elif self.namespace.action == Actions.download:
            self.runDownload()
        elif self.namespace.action == Actions.list:
            if self.namespace.files_only and self.namespace.directories_only:
                return self.list_parser.error("Only -d or -f is allowed at the same time.")
            self.runList()
        elif self.namespace.action == Actions.statistic:
            self.runStatistic()
        elif self.namespace.action == Actions.wipe:
            self.runWipe()
        elif self.namespace.action == Actions.remove:
            self.runRemove()
        elif self.namespace.action == Actions.clean:
            self.runClean()
        elif self.namespace.action == Actions.check:
            self.runCheck()
        elif self.namespace.action == Actions.ftp:
            self.runFTP()
        else:
            self.argparser.print_help()

    def _normalize_path(self, path):
        """
        remove one or multiple . or .. or / at the beginning of a path
        on windows replaces backslash with slash
        """
        return_path = path
        if os.path.isdir(path):
            return_path = os.path.basename(path)
        if os.sep == '\\':
            return_path = return_path.replace('\\', '/')
        if return_path == '.':
            return_path = ''
        if '../' in return_path:
            return_path = os.path.basename(os.path.realpath(return_path))

        # if path == '../':
        #     path = os.path.basename(os.getcwd())
        path_changed = True
        while path_changed:
            if return_path.startswith('./'):
                return_path = return_path[2:]
            elif return_path.startswith('../'):
                return_path = return_path[3:]
            else:
                path_changed = False
                continue
            path_changed = True
        if return_path.endswith('/'):
            return_path = return_path[:-1]
        if return_path.startswith('/'):
            return_path = return_path[1:]
        if self.namespace.prefix:
            if return_path:
                return_path = self.namespace.prefix + '/' + return_path
            else:
                return_path = self.namespace.prefix
        if os.path.isdir(os.path.realpath(path)) and return_path != '':
            return_path += '/'
        print(repr(path), '  ->  ', repr(return_path))
        return return_path

    def runUpload(self):
        # Todo: add info at end for uploaded files, fragments, sizes, etc
        #  give feedback what was done
        if self.namespace.fragment_size:
            assert type(self.namespace.fragment_size) is int, type(self.namespace.fragment_size)
            self.save_service.fragment_size = self.namespace.fragment_size
        if self.namespace.dryrun:
            self.namespace.fragment_policy = 'pass'
        # print('???', self.save_service.compress_type, self.namespace.compress1)
        self.save_service.compress_type = self.namespace.compress1
        assert self.save_service.compress_type == self.namespace.compress1
        # self.save_service.compress_type = self.namespace.compress1
        self.save_service.fragment_cache.resource_compress_type = self.namespace.compress2
        assert self.save_service.fragment_cache.resource_compress_type == self.namespace.compress2
        if self.namespace.fragment_policy == 'pass':
            self.save_service.fragment_cache.policy = FragmentCache.POLICY_PASS
        elif self.namespace.fragment_policy == 'fill':
            self.save_service.fragment_cache.policy = FragmentCache.POLICY_FILL
        elif self.namespace.fragment_policy == 'fill_always':
            self.save_service.fragment_cache.policy = FragmentCache.POLICY_FILL_ALWAYS
        else:
            raise NotImplementedError(repr(self.namespace))

        if self.namespace.prefix:
            self.namespace.prefix = self.namespace.prefix.rstrip('/')
        if self.namespace.fragment_size:
            self._check_namespace_fragment_size()

        with self.save_service:
            if self.namespace.stdin:
                name = self.namespace.item[0].pop(0)
                if self.namespace.prefix:
                    name = self.namespace.prefix + '/' + name
                if not self.namespace.silent:
                    print('uploading ' + name + ' from stdin', file=sys.stderr)  # + '"')# as "' + keyname + '"')
                dest_path = os.path.dirname(name)
                dest_file_name = os.path.basename(name)
                self.is_fs.makedirs(dest_path, recreate=True)
                with self.is_fs.opendir(dest_path) as dst_fs:
                    dest_file_exists = dst_fs.exists(dest_file_name)
                    if dest_file_exists and not self.namespace.overwrite:
                        if not self.namespace.silent:
                            print('"' + name + '" already uploaded', file=sys.stderr)
                        return
                    with TqdmUpTo(unit='Bytes',
                                  unit_scale=True,
                                  disable=self.namespace.silent) as progressreporter:
                        self._set_frag_cache_on_upload_callback(progressreporter)
                        with dst_fs.open(dest_file_name, 'wb') as dst_file:
                            for index, chunk in enumerate(
                                    iter(lambda: sys.stdin.buffer.read(self.save_service.fragment_size) or None, None)):
                                dst_file.write(chunk)
                                # progressreporter.update_to(index+1, self.save_service.fragment_size, total_size)
                                progressreporter.update(len(chunk))
                    self._set_frag_cache_on_upload_printer()

                # with TqdmUpTo(  # desc='uploading "' + path + '"',# as "' + keyname + '"',
                #         unit='Bytes',
                #         unit_scale=True,
                #         disable=self.namespace.silent) as progressreporter:
                #     self._set_frag_cache_on_upload_callback(progressreporter)
                #     # Todo: replace with fs based file upload
                #     try:
                #         self.save_service.saveStream(sys.stdin.buffer, name, overwrite=self.namespace.overwrite,
                #                                      progressreporter=progressreporter,
                #                                      fragment_size=self.namespace.fragment_size,
                #                                      )
                #     except CompoundAlreadyExistsException:
                #         progressreporter.display(name + " already uploaded")
                self._unset_frag_cache_on_upload_callback()
            mix_items = self.namespace.item[0]  # type: List[str]
            globbed_items = []  # type: List[str]
            invalid_glob = []  # type: List[str]
            for item in mix_items:
                globitems = glob.glob(item)
                if globitems:
                    globbed_items.extend(globitems)
                else:
                    invalid_glob.append(item)
            for not_found in invalid_glob:
                print(not_found, "does not exist", file=sys.stderr)
            if invalid_glob:
                exit(1)
            globbed_items = set(globbed_items)  # type: Set[str]

            for item_index, globbed_item in enumerate(globbed_items):
                self.is_fs.flush()
                if os.sep == '\\':
                    globbed_item = globbed_item.replace('\\', '/')
                if globbed_item.endswith('/'):
                    globbed_item = globbed_item[0:-1]
                if os.path.isfile(globbed_item):
                    if self._is_skippable(globbed_item, self.namespace.exclude):
                        if not self.namespace.silent:
                            print('skipping (' + str(item_index + 1) + ' of ' + str(
                                len(globbed_items)) + ') ' + globbed_item, file=sys.stderr)
                        continue
                    try:
                        # print(globbed_item, os.path.dirname(globbed_item))
                        # print(globbed_item, os.path.basename(globbed_item))
                        with OSFS(os.path.dirname(globbed_item)) as src_fs:
                            src_file_name = '/' + os.path.basename(globbed_item)
                            # src_fs.tree(max_levels=1)
                            with src_fs.open(src_file_name, 'rb') as src_file:
                                dest_file_name = os.path.basename(os.path.abspath(globbed_item))
                                if self.namespace.prefix:
                                    dest_file_name = self.namespace.prefix + '/' + dest_file_name
                                else:
                                    dest_file_name = dest_file_name
                                self._upload_file(dest_file_name, globbed_item, src_file_name, src_fs, src_file,
                                                  self.is_fs, '(' + str(item_index + 1) + ' of ' + str(
                            len(globbed_items)) + ') ')
                    except PermissionError as e:
                        print("Unable to open file,", str(e),file=sys.stderr)
                elif os.path.isdir(globbed_item) and not self.namespace.recursive:
                    if self._is_skippable(globbed_item, self.namespace.exclude):
                        if not self.namespace.silent:
                            print('skipping Directory', globbed_item, file=sys.stderr)
                        continue
                    dest_dir_name = os.path.basename(os.path.abspath(globbed_item))
                    if self.namespace.prefix:
                        dest_dir_name = self.namespace.prefix + '/' + dest_dir_name

                    if self.is_fs.exists(dest_dir_name):
                        if not self.namespace.silent:
                            print(globbed_item, "already indexed", file=sys.stderr)
                        continue
                    else:
                        self.is_fs.makedirs(dest_dir_name)
                        if not self.namespace.silent:
                            print("indexed Directory", globbed_item, file=sys.stderr)
                elif os.path.isdir(globbed_item) and self.namespace.recursive:
                    if self._is_skippable(globbed_item, self.namespace.exclude):
                        if not self.namespace.silent:
                            print('skipping (' + str(item_index + 1) + ' of ' + str(
                                len(globbed_items)) + ') ' + globbed_item, file=sys.stderr)
                        continue
                    dest_dir_name = os.path.basename(os.path.abspath(globbed_item))
                    if self.namespace.prefix:
                        dest_dir_name = self.namespace.prefix + '/' + dest_dir_name
                    # open source directory
                    # open/mount the corresponding IS-FS directory
                    self.is_fs.makedirs(dest_dir_name, recreate=True)
                    with OSFS(globbed_item) as src_fs, self.is_fs.opendir(dest_dir_name) as dst_fs:
                        # input('BREAKPOINT, enter enter to continue')
                        # local_dirs = set(src_fs.walk.dirs())
                        local_dirs = list(src_fs.walk.dirs())
                        # local_dirs = sorted(local_dirs, key=lambda i: i.count('/'))
                        for local_dir_index, local_dir in enumerate(local_dirs):
                            globbed_dest_dir_name = globbed_item + '/' + local_dir[1:]
                            if self._is_skippable(globbed_dest_dir_name, self.namespace.exclude):
                                if not self.namespace.silent:
                                    print('(' + str(item_index + 1) + ' of ' + str(
                                        len(globbed_items)) + ', ' + str(local_dir_index + 1) + ' of ' + str(
                                        len(local_dirs)) + ') skipping "' + globbed_dest_dir_name + '"', file=sys.stderr
                                          )
                                continue
                            if dst_fs.exists(local_dir):
                                if not self.namespace.silent:
                                    print('(' + str(item_index + 1) + ' of ' + str(
                                        len(globbed_items)) + ', ' + str(local_dir_index + 1) + ' of ' + str(
                                        len(local_dirs)) + ') already indexed "' + globbed_dest_dir_name + '"',
                                          file=sys.stderr)
                            else:
                                if not self.namespace.silent:
                                    print('(' + str(item_index + 1) + ' of ' + str(
                                        len(globbed_items)) + ', ' + str(local_dir_index + 1) + ' of ' + str(
                                        len(local_dirs)) + ') indexing "' + globbed_dest_dir_name + '"', file=sys.stderr
                                          )
                                dst_fs.makedirs(local_dir, recreate=True)
                        # local_files = set(src_fs.walk.files())
                        local_files = list(src_fs.walk.files())
                        for local_file_index, local_file in enumerate(local_files):
                            globbed_dest_file_name = globbed_item + '/' + local_file[1:]
                            if self._is_skippable(globbed_dest_file_name, self.namespace.exclude):
                                if not self.namespace.silent:
                                    print('(' + str(item_index + 1) + ' of ' + str(
                                        len(globbed_items)) + ', ' + str(local_file_index + 1) + ' of ' + str(
                                        len(local_files)) + ') skipping "' + globbed_dest_file_name + '"', file=sys.stderr)
                                continue
                            # print(globbed_item, local_file, fs.path.join(globbed_item, local_file))
                            try:
                                src_file = src_fs.open(local_file, 'rb')
                            except (ResourceNotFound, FileExpected) as e:
                                print('(' + str(item_index + 1) + ' of ' + str(
                                    len(globbed_items)) + ', ' + str(local_file_index + 1) + ' of ' + str(
                                    len(local_files)) + ') file vanished "' + globbed_dest_file_name + '";', repr(e), file=sys.stderr)
                                continue
                            except PermissionDenied as e:
                                print('(' + str(item_index + 1) + ' of ' + str(
                                    len(globbed_items)) + ', ' + str(local_file_index + 1) + ' of ' + str(
                                    len(local_files)) + ') permission denied "' + globbed_dest_file_name + '";', repr(e), file=sys.stderr)
                                continue

                            try:
                                self._upload_file(local_file, globbed_dest_file_name, local_file, src_fs, src_file,
                                                  dst_fs, '(' + str(item_index + 1) + ' of ' + str(
                len(globbed_items)) + ', ' + str(local_file_index + 1) + ' of ' + str(
                len(local_files)) + ') ')
                            finally:
                                src_file.close()

                        # print('chars local dirs', sum((len(i) for i in local_dirs)))
                        # print('chars local files', sum((len(i) for i in local_files)))

                        if self.namespace.sync:
                            self.save_service.flush()

                            remote_files = set(dst_fs.walk.files())
                            # print('chars remote files', sum((len(i) for i in remote_files)))

                            deletable_files = remote_files.difference(local_files)
                            if len(deletable_files) > 0:
                                with TqdmUpTo(deletable_files, desc='removing Files',
                                              unit='File',
                                              unit_scale=True,
                                              disable=self.namespace.silent,
                                              total=len(deletable_files)) as progressreporter:
                                    for deletable_file in progressreporter:
                                        dst_fs.remove(deletable_file)
                                        progressreporter.write('removed "' + deletable_file + '"')
                            remote_dirs = set(dst_fs.walk.dirs())
                            # print('chars remote dirs', sum((len(i) for i in remote_dirs)))
                            deletable_dirs = remote_dirs.difference(local_dirs)
                            deletable_dirs = sorted(deletable_dirs, key=lambda k: k.count('/'), reverse=True)
                            if len(deletable_dirs) > 0:
                                with TqdmUpTo(deletable_dirs, desc='removing Directories',
                                              unit='Dir',
                                              unit_scale=True,
                                              disable=self.namespace.silent,
                                              total=len(deletable_dirs)) as progressreporter:
                                    for deletable_dir in progressreporter:
                                        try:
                                            dst_fs.removetree(deletable_dir)
                                            progressreporter.write('removed "' + deletable_dir + '"')
                                        except ResourceNotFound:
                                            progressreporter.write('already removed "' + deletable_dir + '"')

            self._set_frag_cache_on_upload_printer()

    def _upload_file(self, dest_file_name, globbed_src_file_name, src_file_name, src_fs, src_file, dst_fs, print_prefix=''):
        # type: (str, str, str, FS, IO, FS, str) -> None
        dest_file_exists = dst_fs.exists(dest_file_name)
        if dest_file_exists and not self.namespace.overwrite:
            if not self.namespace.silent:
                print(print_prefix+'already uploaded "' + globbed_src_file_name + '"', file=sys.stderr)
            return
        elif dest_file_exists and self.namespace.overwrite and self.namespace.update:
            src_hash = src_fs.hash(src_file_name, 'sha256')
            dst_hash = dst_fs.hash(dest_file_name, 'sha256')
            if src_hash == dst_hash:
                if not self.namespace.silent:
                    print(print_prefix+'already uploaded "' + globbed_src_file_name + '"', file=sys.stderr)
                return
        elif dest_file_exists and self.namespace.overwrite and not self.namespace.update:
            pass
            # if not self.namespace.silent:
            #     print(print_prefix+'already uploaded "' + globbed_src_file_name + '"', file=sys.stderr)
            # return
        else:
            pass
        if not self.namespace.silent:
            print(print_prefix+'uploading "' + globbed_src_file_name + '"', file=sys.stderr)

        total_size = get_size_of_stream(cast(BinaryIO, src_file))
        with TqdmUpTo(unit='Bytes', total=total_size,
                      unit_scale=True,
                      disable=self.namespace.silent) as progressreporter:
            dst_fs.makedirs(fs.path.dirname(dest_file_name), recreate=True)
            self._set_frag_cache_on_upload_callback(progressreporter)
            with dst_fs.open(dest_file_name, 'wb') as dst_file:
                for index, chunk in enumerate(
                        iter(lambda: src_file.read(self.save_service.fragment_size) or None, None)):
                    dst_file.write(chunk)
                    # progressreporter.update_to(index+1, self.save_service.fragment_size, total_size)
                    progressreporter.update(len(chunk))
        self._set_frag_cache_on_upload_printer()

    def _download_file_to_target(self, src_fs, filepath, dst_fs):
        # type: (FS, str, FS) -> None
        filesize = src_fs.getsize(filepath)
        with src_fs.open(filepath, 'rb') as src_file:
            dst_fs.makedirs(fs.path.dirname(filepath), recreate=True)
            with dst_fs.open(filepath, 'wb') as dst_file:
                self._download_file_to_targetfile(src_file, dst_file, filesize)
            # assert src_fs.hash(filepath, 'sha256') == dst_fs.hash(filepath, 'sha256')

    def _download_file_to_targetfile(self, src_file, dst_file, filesize):
        # type: (IO, IO, int) -> None
        with TqdmUpTo(unit='Bytes', total=filesize,
                      unit_scale=True,
                      disable=self.namespace.silent) as progressreporter:
            # progressreporter.write('downloading "' + filepath + '" ')
            self._set_frag_cache_on_download_callback(progressreporter)
            for index, chunk in enumerate(
                    iter(lambda: src_file.read(self.save_service.fragment_size) or None, None)):
                dst_file.write(chunk)
                # progressreporter.update_to(index+1, self.save_service.fragment_size, total_size)
                progressreporter.update(len(chunk))
        self._unset_frag_cache_on_download_callback()

    def runDownload(self):
        if self.namespace.target == '-' and len(self.namespace.item[0]) > 1:
            self.download_parser.error('cannot download and write multiple items to stdout')
            return

        mix_items = self.namespace.item[0]  # type: List[str]
        # noinspection PyProtectedMember
        mix_items_patterns = {i: fnmatch._compile_pattern(i) for i in mix_items}
        downloadable_dirs = []
        downloadable_files = []

        for item in list(mix_items):
            if self.save_service.hasCompoundWithName(item):
                compound = self.save_service.getCompoundWithName(item)
                if self._is_skippable_exclude_list(compound.compound_name, self.namespace.exclude):
                    continue
                if compound.compound_type == Compound.DIR_TYPE:
                    downloadable_dirs.append(compound.compound_name)
                else:
                    downloadable_files.append(compound.compound_name)
                    if self.namespace.target == '-' and len(downloadable_files) > 1:
                        self.download_parser.error('cannot download and write multiple files to stdout')
                        return
                mix_items.remove(item)
                mix_items_patterns.pop(item)

        if mix_items:
            if not self.namespace.advanced_globbing:
                compounds = self.save_service.listCompounds()
                for compound in compounds:
                    if self._is_skippable_exclude_list(compound.compound_name, self.namespace.exclude):
                        continue
                    for mix_item in mix_items:
                        if mix_items_patterns[mix_item](compound.compound_name):
                        # if fnmatch.fnmatch(compound.compound_name, mix_item):
                            if compound.compound_type == Compound.DIR_TYPE:
                                downloadable_dirs.append(compound.compound_name)
                            else:
                                downloadable_files.append(compound.compound_name)
                                if self.namespace.target == '-' and len(downloadable_files) > 1:
                                    self.download_parser.error('cannot download and write multiple files to stdout')
                                    return
            else:
                for mix_item in mix_items:
                    for match in self.is_fs.glob(mix_item):
                        if self._is_skippable_exclude_list(match.path, self.namespace.exclude):
                            continue
                        if self.is_fs.isdir(match.path):
                            downloadable_dirs.append(match.path)
                        else:
                            downloadable_files.append(match.path)
                            if self.namespace.target == '-' and len(downloadable_files) > 1:
                                self.download_parser.error('cannot download and write multiple files to stdout')
                                return
        if len(downloadable_files) == 0 and len(downloadable_dirs) == 0:
            self.download_parser.error("no matching compounds found")
            return

            # for compound in self.save_service.listCompounds():
            #     if self._is_skippable_exclude_list(compound.compound_name, self.namespace.exclude):
            #         continue
            #     for item in mix_items:
            #         if mix_items_patterns[item](compound.compound_name) is not None:
            #             if compound.compound_type == Compound.DIR_TYPE:
            #                 downloadable_dirs.append(compound.compound_name)
            #             else:
            #                 downloadable_files.append(compound.compound_name)
            #                 if self.namespace.target == '-' and len(downloadable_files) > 1:
            #                     self.download_parser.error('cannot download and write multiple files to stdout')
            #                     return

        if self.namespace.target == '-':
            assert len(downloadable_files) == 1
            filename = downloadable_files[0]
            with self.is_fs.open(filename, 'rb') as src_file:
                filesize = self.is_fs.getsize(filename)
                self._download_file_to_targetfile(src_file, sys.stdout.buffer, filesize)
                sys.stdout.buffer.flush()
                return

        try:
            dst_fs = OSFS(self.namespace.target)
        except CreateFailed as e:
            print("unable to open target;", str(e), file=sys.stderr)
            return

        with dst_fs:
            for dirindex, dirname in enumerate(downloadable_dirs):
                if dst_fs.exists(dirname):
                    print('(' + str(dirindex + 1) + ' of ' + str(len(downloadable_dirs))
                          + ') skipping "' + dirname + '"', file=sys.stderr)
                else:
                    print('(' + str(dirindex + 1) + ' of ' + str(len(downloadable_dirs))
                          + ') creating "' + dirname + '"', file=sys.stderr)
                    dst_fs.makedirs(dirname, recreate=True)

            for fileindex, filename in enumerate(downloadable_files):
                if dst_fs.exists(filename) and not self.namespace.overwrite:
                    print('(' + str(fileindex + 1) + ' of ' + str(len(downloadable_files))
                          + ') skipping "' + filename + '"', file=sys.stderr)
                else:
                    print('(' + str(fileindex + 1) + ' of ' + str(len(downloadable_files))
                          + ') downloading "' + filename + '"', file=sys.stderr)
                    self._download_file_to_target(self.is_fs, filename, dst_fs)

    def runDownloadOLD(self):
        if self.namespace.target == '-' and len(self.namespace.item[0]) > 1:
            self.download_parser.error('cannot download and write multiple items to stdout')
            return
        mix_times = self.namespace.item[0]  # type: List[str]
        download_names = []
        for item in list(mix_times):
            if self.save_service.hasCompoundWithName(item):
                download_names.append(item)
                mix_times.remove(item)
        # saved_names = [c.compound_name for c in self.save_service.listCompounds()]
        # for item in mix_times:
        #     if item in saved_names:
        #         download_names.append(item)
        #     else:
        #         filtered_items = fnmatch.filter(saved_names, item)
        #         if len(filtered_items) == 0:
        #             print(repr(item), "is not saved on Target")
        #         else:
        #             download_names.extend(filtered_items)
        if len(mix_times) > 0:
            successful_items = {i: 0 for i in mix_times}
            for compound in self.save_service.listCompounds():
                for item in mix_times:
                    if item == compound.compound_name:
                        download_names.append(compound.compound_name)
                        successful_items[item] += 1
                        break
                    else:
                        if fnmatch.fnmatch(compound.compound_name, item):
                            download_names.append(compound.compound_name)
                            successful_items[item] += 1
                            break
            for item, count in successful_items.items():
                if count == 0:
                    print(repr(item), "is not saved on Target")

        # download_names = list(set(download_names))

        if self.namespace.target == '-' and len(download_names) > 1:
            self.download_parser.error('cannot download and write multiple items to stdout')
            return

        for item in download_names:
            if self.namespace.target == '-':
                f = sys.stdout.buffer
                with TqdmUpTo(  # desc='uploading "' + path + '"',# as "' + keyname + '"',
                        unit='Bytes',
                        unit_scale=True,
                        disable=self.namespace.silent) as progressreporter:
                    data_generator = self.save_service.loadCompound(item,
                                                                    progressreporter=progressreporter
                                                                    )
                    for d in data_generator:
                        f.write(d)
                continue

            target = os.path.join(self.namespace.target, item)
            target_dir = os.path.dirname(target)
            if os.sep == '\\':
                target = target.replace('/', '\\')
                target_dir = target_dir.replace('/', '\\')
            if item.endswith('/'):
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                    print("downloaded Directory", item, "to", target_dir)
                else:
                    if self.namespace.verbose:
                        print("skipping existing Directory", item, '@', target_dir)
            elif not item.endswith('/'):
                if os.path.exists(target) and not self.namespace.overwrite:
                    # if self.namespace.verbose:
                    print("skipping existing File", item, '@', target)
                    continue
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                with open(target, 'wb') as f:
                    print('downloading "' + item + '" ')
                    # bar = TimeRemainingBar()
                    # progressreporter = ProgressReporter(
                    #     widgetProgressCallback=lambda p, t, c: bar.progress(p, postCountdown=' (' + str(
                    #         int(c)) + ' of ' + str(
                    #         int(t)) + ' Fragments)'))
                    with TqdmUpTo(  # desc='uploading "' + path + '"',# as "' + keyname + '"',
                            unit='Bytes',
                            unit_scale=True,
                            disable=self.namespace.silent) as progressreporter:
                        data_generator = self.save_service.loadCompound(item,
                                                                        progressreporter=progressreporter
                                                                        )
                        for d in data_generator:
                            f.write(d)
                    # bar.finalize()
            else:
                raise Exception(repr(item))

    # noinspection DuplicatedCode
    def runList(self):
        filter_items = self.namespace.filter if self.namespace.filter else []
        if len(filter_items) == 0:
            if self.namespace.tree:
                self.is_fs.tree(max_levels=self.namespace.tree_depth)
                return
            if self.namespace.files_only:
                compound_type = Compound.FILE_TYPE
            elif self.namespace.directories_only:
                compound_type = Compound.DIR_TYPE
            else:
                compound_type = None
            compounds = self.save_service.listCompounds(type_filter=compound_type,
                                                        order_alphabetically=True)
            # compounds = (c.compound_name for c in compounds)
            for compound in compounds:
                out = compound.compound_name
                if self.namespace.checksum:
                    out += '    ' + compound.compound_hash.hex()
                if self.namespace.size:
                    out += '    (' + str(compound.compound_size) + ' / ' + fromBytes(compound.compound_size) + ')'
                print(out)
        else:
            for item in list(filter_items):
                if self.namespace.tree:
                    if not self.is_fs.isdir(item):
                        print("'" + item + "' does not exist")
                        return
                    print("Tree of Directory \"" + item + "\"")
                    subfs = self.is_fs.opendir(item)
                    subfs.tree(max_levels=self.namespace.tree_depth)
                    filter_items.remove(item)
                    continue
                if self.save_service.hasCompoundWithName(item):
                    compound = self.save_service.getCompoundWithName(item)
                    printable = False
                    if self.namespace.files_only:
                        if compound.compound_type == Compound.FILE_TYPE:
                            printable = True
                    elif self.namespace.directories_only:
                        if compound.compound_type == Compound.DIR_TYPE:
                            printable = True
                    else:
                        printable = True
                    if printable:
                        out = compound.compound_name
                        if self.namespace.checksum:
                            out += '    ' + compound.compound_hash.hex()
                        if self.namespace.size:
                            out += '    (' + str(compound.compound_size) + ' / ' + fromBytes(
                                compound.compound_size) + ')'
                        print(out)
                    filter_items.remove(item)
            # if self.namespace.filter:
            #     compound_names = (n for n in compound_names if fnmatch.fnmatch(n, self.namespace.filter))

            # compound_names = [c.compound_name for c in self.save_service.listCompounds(order_alphabetically=True)]
            # if self.namespace.filter:
            #     compound_names = fnmatch.filter(compound_names, self.namespace.filter)
            #
            #
            # # print "sorting..."
            # # compound_names = [c.compoundName for c in compounds]
            # compound_names.sort(key=str.lower)  # sort alphabetically
            # # sorted(compound_names, cmp=lambda x, y: -1 if x.count('/') > 1 >= y.count('/') else 0)
            # # compound_names.sort(cmp=lambda x, y: -1 if x.count('/') > 1 >= y.count('/') else 0)  # sort root files to end
            if len(filter_items) > 0:
                if not self.namespace.advanced_globbing:
                    if self.namespace.files_only:
                        compound_type = Compound.FILE_TYPE
                    elif self.namespace.directories_only:
                        compound_type = Compound.DIR_TYPE
                    else:
                        compound_type = None
                    compounds = self.save_service.listCompounds(type_filter=compound_type,
                                                                order_alphabetically=True)
                    for compound in compounds:
                        for mix_item in filter_items:

                            if fnmatch.fnmatch(compound.compound_name, mix_item):
                                out = compound.compound_name
                                if self.namespace.checksum:
                                    out += '    ' + compound.compound_hash.hex()
                                if self.namespace.size:
                                    out += '    (' + str(compound.compound_size) + ' / ' + fromBytes(
                                        compound.compound_size) + ')'
                                print(out)
                else:
                    for mix_item in filter_items:
                        for match in self.is_fs.glob(mix_item):
                            if self.namespace.files_only and not match.info.is_file:
                                continue
                            elif self.namespace.directories_only and not match.info.is_dir:
                                continue
                            out = match.path
                            if self.namespace.checksum:
                                out += '    ' + self.is_fs.hash(match.path, 'sha256')
                            if self.namespace.size:
                                out += '    (' + str(self.is_fs.getsize(match.path)) + ' / ' + fromBytes(
                                    self.is_fs.getsize(match.path)) + ')'
                            print(out)

    def runStatistic(self):
        with self.save_service:
            print("saved Compounds                                ",
                  self.save_service.getTotalCompoundCount())
            print("saved Compounds (Files)                        ",
                  self.save_service.getTotalCompoundCount(with_type=Compound.FILE_TYPE))
            print("saved Compounds (Directories)                  ",
                  self.save_service.getTotalCompoundCount(with_type=Compound.DIR_TYPE))
            print("saved unique Compounds                         ",
                  self.save_service.getUniqueCompoundCount())
            print("saved Fragments                                ",
                  self.save_service.getTotalFragmentCount())
            print("saved Resources on target (referenced)         ",
                  self.save_service.getTotalResourceCount())
            if self.namespace.offline:
                total_resource_count = 0
            else:
                total_resource_count = len(self.save_service.storage.listResourceNames())
            print("saved Resources on target (total)              ", total_resource_count)

            print("multiple used fragments                        ",
                  self.save_service.getMultipleUsedFragmentsCount())
            size_bytes = self.save_service.getSavedBytesByMultipleUsedFragments()
            size_pretty = fromBytes(size_bytes)
            print("saved space by multiple used fragments (Bytes) ", size_bytes)
            print("saved space by multiple used fragments (pretty)", size_pretty)

            print("multiple used compounds (Files)                ",
                  self.save_service.getMultipleUsedCompoundsCount(Compound.FILE_TYPE))
            size_bytes = self.save_service.getSavedBytesByMultipleUsedCompounds()
            size_pretty = fromBytes(size_bytes)
            print("saved space by multiple used compounds (Bytes) ", size_bytes)
            print("saved space by multiple used compounds (pretty)", size_pretty)

            size_bytes = self.save_service.getTotalResourceSize()
            size_pretty = fromBytes(size_bytes)
            print("total Resource Size on target (Bytes)          ", size_bytes)
            print("total Resource Size on target (pretty)         ", size_pretty)
            size_bytes = self.save_service.getTotalFragmentSize()
            size_pretty = fromBytes(size_bytes)
            print("total Fragment Size (Bytes)                    ", size_bytes)
            print("total Fragment Size (pretty)                   ", size_pretty)
            size_bytes = self.save_service.getUniqueCompoundSize()
            size_pretty = fromBytes(size_bytes)
            print("unique Compounds Size (Bytes)                  ", size_bytes)
            print("unique Compounds Size (pretty)                 ", size_pretty)
            size_bytes = self.save_service.getTotalCompoundSize()
            size_pretty = fromBytes(size_bytes)
            print("total Compounds Size (Bytes)                   ", size_bytes)
            print("total Compounds Size (pretty)                  ", size_pretty)
            print("unneeded Fragments                             ",
                  self.save_service.getUnneededFragmentCount())
            size_bytes = self.save_service.getUnneededFragmentSize()
            size_pretty = fromBytes(size_bytes)
            print("unneeded Fragments Size (Bytes)                ", size_bytes)
            print("unneeded Fragments Size (pretty)               ", size_pretty)

    def runWipe(self):
        self.save_service.wipeAll(collect_garbage=False)
        self.namespace.optimize_space = None
        self.namespace.optimize_fullness = False
        self.namespace.keep_fragments = False
        self.namespace.keep_resources = False
        self.namespace.keep_unreferenced_resources = False
        self.namespace.defragment = False

        if self.namespace.clean:
            self.runClean()

    def runClean(self):
        with self.save_service:
            if self.namespace.optimize_space is not None:
                with TqdmUpTo(desc='removing Fragments',
                              unit='Fragment',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter_fragments:
                    self.save_service.collectGarbage(keep_fragments=self.namespace.keep_fragments,
                                                     keep_resources=True,
                                                     keep_unreferenced_resources=True,
                                                     progressreporter_fragments=progressreporter_fragments)
                with TqdmUpTo(desc='removing unused space on Resources',
                              unit='Resource',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter:
                    self._set_frag_cache_on_upload_callback(progressreporter)
                    self.save_service.optimizeResourceSpace(unused_percentage=self.namespace.optimize_space,
                                                            progressreporter=progressreporter)
            elif self.namespace.optimize_fullness:
                with TqdmUpTo(desc='removing Fragments',
                              unit='Fragment',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter_fragments:
                    self.save_service.collectGarbage(keep_fragments=self.namespace.keep_fragments,
                                                     keep_resources=True,
                                                     keep_unreferenced_resources=True,
                                                     progressreporter_fragments=progressreporter_fragments)
                with TqdmUpTo(desc='optimizing Resource contents',
                              unit='Resource',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter:
                    self._set_frag_cache_on_upload_callback(progressreporter)
                    self.save_service.optimizeResourceUsage(progressreporter=progressreporter)
            elif self.namespace.defragment:
                with TqdmUpTo(desc='removing Fragments',
                              unit='Fragment',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter_fragments:
                    self.save_service.collectGarbage(keep_fragments=self.namespace.keep_fragments,
                                                     keep_resources=True,
                                                     keep_unreferenced_resources=True,
                                                     progressreporter_fragments=progressreporter_fragments)
                with TqdmUpTo(desc='defragmenting Resources',
                              unit='Fragment',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter:
                    self._set_frag_cache_on_upload_callback(progressreporter)
                    self.save_service.defragmentResources(progressreporter=progressreporter)
                # self.save_service.optimizeResourceSpace()
                # self.save_service.collectGarbage(keep_fragments=True,
                #                                  keep_resources=self.namespace.keep_resources,
                #                                  keep_unreferenced_resources=True)
            else:
                with TqdmUpTo(desc='removing Fragments',
                              unit='Fragment',
                              unit_scale=True,
                              disable=self.namespace.silent) as progressreporter_fragments:
                    self.save_service.collectGarbage(keep_fragments=self.namespace.keep_fragments,
                                                     keep_resources=True,
                                                     keep_unreferenced_resources=True,
                                                     progressreporter_fragments=progressreporter_fragments)
            self._set_frag_cache_on_upload_printer()
        with TqdmUpTo(desc='removing Resources',
                      unit='Resource',
                      unit_scale=True,
                      disable=self.namespace.silent) as progressreporter:
            self.save_service.collectGarbage(keep_fragments=True,
                                             keep_resources=self.namespace.keep_resources,
                                             keep_unreferenced_resources=self.namespace.keep_unreferenced_resources,
                                             progressreporter_resources=progressreporter)

    def runRemove(self):
        mix_items = self.namespace.item[0]  # type: List[str]
        matching_names = []

        for item in list(mix_items):
            if self.save_service.hasCompoundWithName(item):
                matching_names.append(item)
                mix_items.remove(item)

        if len(mix_items) > 0:
            if not self.namespace.advanced_globbing:
                compounds = self.save_service.listCompounds()
                for compound in compounds:
                    if self._is_skippable_exclude_list(compound.compound_name, self.namespace.exclude):
                        continue
                    for mix_item in mix_items:
                        if fnmatch.fnmatch(compound.compound_name, mix_item):
                            matching_names.append(compound.compound_name)
            else:
                for mix_item in mix_items:
                    for match in self.is_fs.glob(mix_item):
                        # if self.is_fs.isdir(match.path):
                        #     for path in itertools.chain(self.is_fs.walk.files(match.path),
                        #                                 self.is_fs.walk.dirs(match.path)):
                        #         if self._is_skippable_exclude_list(path, self.namespace.exclude):
                        #             continue
                        #         matching_names.append(path)
                        # else:
                        match_path = match.path  # type: str
                        if match_path.endswith('/'):
                            match_path = match_path[:-1]
                        if self._is_skippable_exclude_list(match_path, self.namespace.exclude):
                            continue
                        matching_names.append(match_path)
        # if not self.namespace.advanced_globbing:
        with TqdmUpTo(matching_names, desc='removing Compounds',
                      unit='Compound',
                      unit_scale=True,
                      disable=self.namespace.silent,
                      total=len(matching_names)) as progressreporter:
            for name in progressreporter:
                progressreporter.write('removing ' + name)
                try:
                    self.save_service.deleteCompound(name)
                except CompoundNotExistingException:
                    progressreporter.write('compound ' + name + ' is missing')
        # else:
        #     removable_paths = []
        #     for name in matching_names:
        #         if self.is_fs.isdir(name):
        #             removable_paths.extend(list(self.is_fs.walk(name).dirs()))
        #         else:
        #             removable_paths.append(name)
        #     with TqdmUpTo(matching_names, desc='removing Paths',
        #                   unit='Path',
        #                   unit_scale=True,
        #                   disable=self.namespace.silent,
        #                   total=len(matching_names)) as progressreporter:
        #         for name in progressreporter:
        #             progressreporter.write('removing ' + name)
        #             if self.is_fs.isdir(name):
        #                 self.is_fs.removetree(name)
        #             else:
        #                 self.is_fs.remove(name)

    def runCheck(self):
        self.save_service.checkStorageConsistency()
        print("Storage has all Referenced Resources by Name")
        self.save_service.checkMetaConsistencyResourcelessFragments()
        print("Meta has all fragments saved in resources")
        self.save_service.checkMetaConsistencyFragmentlessCompounds()
        print("Meta has all fragments needed for compounds")

        if self.namespace.consistency_resourcedata:
            with TqdmUpTo(desc='checking Resource Data',
                          unit='Resource',
                          unit_scale=True,
                          disable=self.namespace.silent
                          ) as progressreporter:
                self.save_service.checkStorageConsistencyByStorageContent(progressreporter)
        if self.namespace.consistency_compounddata:
            with TqdmUpTo(desc='Checking Compound Data',
                          unit='Byte',
                          unit_scale=True,
                          disable=self.namespace.silent
                          ) as progressreporter:
                self._set_frag_cache_on_download_callback(progressreporter)
                self.save_service.checkConsistencyOfAllCompounds(progressreporter)
                self._unset_frag_cache_on_download_callback()

        #
        # ###
        # mix_items = self.namespace.item[0]  # type: List[str]
        # saved_names = [c.compound_name for c in self.save_service.listCompounds()]
        #
        # globbed_items = []
        # for mix_item in mix_items:
        #     filtered_items = fnmatch.filter(saved_names, mix_item)
        #     if not filtered_items:
        #         print(repr(mix_item), "is not saved on Target")
        #     else:
        #         globbed_items.extend(filtered_items)
        # globbed_items = list(set(globbed_items))
        #
        # for mix_item in globbed_items:
        #     self.save_service.deleteCompound(mix_item)
        #     print("removed", mix_item)

    def runFTP(self):
        from ImageSaverLib4.FTPServer import serve_fs
        saver = self.save_service
        is_fs = self.is_fs
        with saver:
            with is_fs:
                self._set_frag_cache_on_upload_printer()
                self._set_frag_cache_on_download_printer()
                try:
                    print("starting FTP server")
                    serve_fs(is_fs, self.namespace.address, self.namespace.port)
                except KeyError:
                    print("catching CTRL+C, flushing saver and closing")
                    saver.flush()
                else:
                    print("flushing saver and closing")
                    saver.flush()

    @staticmethod
    def _winPath2ubuPath(winpath):
        # type: (str) -> str
        d, p = winpath.split(':')
        ubupath = '/' + d + p.replace('\\', '/')
        return ubupath

    def _is_skippable(self, path, excludelist):
        # type: (str, List[str]) -> bool
        try:
            s = os.stat(path)
        except FileNotFoundError:
            return False
        if excludelist:
            for exclude_item in excludelist:
                if fnmatch.fnmatch(path, exclude_item):
                    # print(path, exclude_item)
                    return True
        if stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode):
            return False
        else:
            # print("skipping path", path, stat.filemode(s.st_mode))
            return True

    def _is_skippable_exclude_list(self, path, excludelist):
        # type: (str, Optional[List[str]]) -> bool
        if excludelist:
            for exclude_item in excludelist:
                if fnmatch.fnmatch(path, exclude_item):
                    # print(path, exclude_item)
                    return True
        return False

    @staticmethod
    def getRelativeFilesFromDir(directory):
        paths = []
        directory_abs_path = os.path.abspath(directory)
        if directory == '.':
            basepath = ''
        else:
            basepath = os.path.basename(directory)
        for (dirpath, x, filenames) in os.walk(directory_abs_path):
            # print dirpath, filenames
            for f in filenames:
                rel_path = os.path.join(dirpath, f).replace(directory_abs_path, '')
                if os.sep == '\\':
                    rel_path = rel_path.replace('\\', '/')
                if rel_path.startswith('/'):
                    rel_path = rel_path[1:]
                rel_path = os.path.join(basepath, rel_path)
                if os.sep == '\\':
                    rel_path = rel_path.replace('\\', '/')
                if rel_path:
                    paths.append((os.path.join(dirpath, f), rel_path))

        return paths

    @staticmethod
    def getRelativeDirsFromDir(directory):
        paths = []
        directory_abs_path = os.path.abspath(directory)

        if directory == '.':
            basepath = ''
        else:
            basepath = os.path.basename(directory)
        for (dirpath, x, filenames) in os.walk(directory_abs_path):
            # print dirpath, filenames
            rel_path = dirpath.replace(directory_abs_path, '')
            if os.sep == '\\':
                rel_path = rel_path.replace('\\', '/')
            if rel_path.startswith('/'):
                rel_path = rel_path[1:]
            rel_path = os.path.join(basepath, rel_path)
            if os.sep == '\\':
                rel_path = rel_path.replace('\\', '/')
            if rel_path:
                paths.append((dirpath, rel_path))

        return paths

    def _unset_frag_cache_on_download_callback(self):
        self.save_service.fragment_cache.onDownload = None

    def _set_frag_cache_on_download_callback(self, progressreporter):
        # type: (TqdmUpTo) -> None
        if not self.namespace.dryrun and self.namespace.verbose:
            self.save_service.fragment_cache.onDownload = lambda res: progressreporter.write(
                "Downloading Resource ("
                + humanfriendly.format_size(res.resource_size)
                + ") ...",
                file=sys.stderr
            )

    def _set_frag_cache_on_download_printer(self):
        # type: () -> None
        if not self.namespace.dryrun:
            if not self.namespace.silent and self.namespace.verbose:
                self.save_service.fragment_cache.onDownload = lambda res: print(
                    "Downloading Resource ("
                    + humanfriendly.format_size(res.resource_size)
                    + ") ...",
                    file=sys.stderr
                )
            else:
                self.save_service.fragment_cache.onDownload = lambda res: None

    def _unset_frag_cache_on_upload_callback(self):
        self.save_service.fragment_cache.onUpload = None

    def _set_frag_cache_on_upload_callback(self, progressreporter):
        # type: (TqdmUpTo) -> None
        if not self.namespace.dryrun and self.namespace.verbose:
            self.save_service.fragment_cache.onUpload = lambda res_size, frag_count: progressreporter.write(
                "Uploading Resource ("
                + humanfriendly.format_size(res_size)
                + ") containing "
                + str(frag_count)
                + (" Fragments..." if frag_count > 1 else " Fragment..."),
                file=sys.stderr
            )

    def _set_frag_cache_on_upload_printer(self):
        # type: () -> None
        if not self.namespace.dryrun:
            if not self.namespace.silent and self.namespace.verbose:
                self.save_service.fragment_cache.onUpload = lambda res_size, frag_count: print(
                    "Uploading Resource ("
                    + humanfriendly.format_size(res_size)
                    + ") containing "
                    + str(frag_count)
                    + (" Fragments..." if frag_count > 1 else " Fragment..."),
                    file=sys.stderr
                )
            else:
                self.save_service.fragment_cache.onUpload = lambda res_size, frag_count: None

    def _check_namespace_fragment_size(self):
        if self.namespace.fragment_size:
            if self.namespace.fragment_size > self.storage.getMaxSupportedResourceSize():
                print("Fragment Size is bigger than Resource Size of Storage!", file=sys.stderr)
                print("To avoid Errors it is recommended to reduce the Fragment Size to the max "
                      "supported Resource Size of the Storage", file=sys.stderr)
        pass


if __name__ == "__main__":
    # debug import to find this damn memory leak
    from pympler import tracker, asizeof
    tr = tracker.SummaryTracker()

    app = ImageSaverApp()
    app.setup()
    app.run()
    app.teardown()

    tr.print_diff()

    app.storage._storage._storage._cache_meta.sessionmaker = None
    app.meta.sessionmaker = None

    s = asizeof.asizeof(app.meta)
    print('size of app.meta', s, humanfriendly.format_size(s))
    s = asizeof.asizeof(app.storage)
    print('size of app.storage', s, humanfriendly.format_size(s))
    s = asizeof.asizeof(app.save_service)
    print('size of app.save_service', s, humanfriendly.format_size(s))
    s = asizeof.asizeof(app.save_service.reserved_compounds)
    print('size of app.save_service.reserved_compounds', s, humanfriendly.format_size(s))
    s = asizeof.asizeof(app.save_service.reserved_fragments)
    print('size of app.save_service.reserved_fragments', s, humanfriendly.format_size(s))
    s = asizeof.asizeof(app.save_service.reserved_resources)
    print('size of app.save_service.reserved_resources', s, humanfriendly.format_size(s))
    app.save_service.fragment_cache.meta = None
    s = asizeof.asizeof(app.save_service.fragment_cache)
    print('size of app.save_service.fragment_cache', s, humanfriendly.format_size(s))