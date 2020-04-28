import traceback
from threading import RLock

import fs
from fs.base import FS
from fs.errors import ResourceNotFound
from fs.info import Info
from fs.memoryfs import MemoryFS

from ImageSaverLib.Errors import CompoundAlreadyExistsException, CompoundNotExistingException
from ImageSaverLib.Helpers.FileLikeIterator import FileLikeIterator
from ImageSaverLib.ImageSaverLib import ImageSaver
from ImageSaverLib.MetaDB.Errors import NotExistingException
from ImageSaverLib.MetaDB.Types.Compound import Compound


class ImageSaverFS(FS):
    def __init__(self, imagesaver, load_fs=False):
        # type: (ImageSaver, bool) -> None
        super().__init__()
        self.debug = False
        self._meta = {
            "network": True,
            "read_only": False,
            "supports_rename": True,
            # changed thread safety to false, because file opening and file closing gets called from different
            # threads (opening from MainThread, closing from worker thread).
            # compound name gets reserved during file opening, which means that a worker thread cannot close it
            # "thread_safe": True,
            "thread_safe": False,
        }

        self.__internal_ram_fs = None
        self._path_builder_lock = RLock()
        self._saver = imagesaver
        try:
            self._saver.saveBytes(b'', '/', compound_type=Compound.DIR_TYPE)
            self._saver.fragment_cache.flush()
            assert self._saver.hasCompoundWithName('/')
        except CompoundAlreadyExistsException:
            pass
        if load_fs:
            self._build_ramfs_from_saver()

    @property
    def _internal_ram_fs(self):
        with self._lock:
            if not self.__internal_ram_fs:
                self.__internal_ram_fs = MemoryFS()
            if self.__internal_ram_fs.isclosed():
                self.__internal_ram_fs = MemoryFS()
            return self.__internal_ram_fs

    def __enter__(self):
        self._saver.__enter__()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        print('ImageSaverFS.__exit__', exc_type, exc_value, traceback)
        try:
            self._saver.__exit__(exc_type, exc_value, traceback)
        except Exception as e:
            print(e)
            print(e.__traceback__)
        finally:
            return super().__exit__(exc_type, exc_value, traceback)

    def _build_ramfs_from_saver(self):
        with self._path_builder_lock:
            for compound in self._saver.listCompounds():
                self._build_ramfs_from_compound(compound)

    def cache_paths(self, path_praefix='/', full_tree=True):
        self._build_partial_ramfs_from_praefix(path_praefix, full_tree)

    def _build_partial_ramfs_from_praefix(self, path_praefix, full_tree=False):
        with self._path_builder_lock:
            try:
                # print("_build_partial_ramfs_from_praefix", path_praefix)
                # print("internal_ram_fs.exists(path_praefix)", self._internal_ram_fs.exists(path_praefix))
                # check if path already exists
                abs_path = self._internal_ram_fs.validatepath(path_praefix)
                if self._internal_ram_fs.exists(abs_path):
                    # print('ramfs.exists', abs_path)
                    # if self._internal_ram_fs.isdir(path_praefix):
                    #     print("internal_ram_fs.isdir(path_praefix)", self._internal_ram_fs.isdir(path_praefix))
                    #     print("internal_ram_fs.listdir(path_praefix)) > 0", self._internal_ram_fs.listdir(path_praefix))
                    # else:
                    #     print("internal_ram_fs.isfile(path_praefix)", self._internal_ram_fs.isfile(path_praefix))
                    # and if its a directory + if it is non empty
                    if self._internal_ram_fs.isdir(abs_path) and len(self._internal_ram_fs.listdir(abs_path)) > 0:
                        return
                    # print('ramfs is not dir or dir is empty', abs_path)
                    # or if its a file, then no more creation is needed
                    if self._internal_ram_fs.isfile(abs_path):
                        return
                    # print('ramfs is not file', abs_path)
                    # path is either a not yet existing file, which means we have to check it with imagesaver
                    # or path is a empty dir, which means we have to build the subtree with imagesavers knowledge

                # print("_build_partial_ramfs_from_praefix", path_praefix)
                # abspath = fs.path.abspath(path_praefix)
                if full_tree:
                    # print('_build_partial_ramfs_from_praefix full_tree=True', abs_path)
                    compound_gen = self._saver.listCompounds(starting_with=abs_path)
                else:
                    # print('_build_partial_ramfs_from_praefix full_tree=False', abs_path)
                    compound_gen = self._saver.listCompounds(starting_with=abs_path, slash_count=abs_path.count('/')+1)
                for compound in compound_gen:
                    # print(compound.compound_name)
                    self._build_ramfs_from_compound(compound)
            except Exception as e:
                traceback.print_tb(e.__traceback__)
                raise

    def _build_ramfs_from_compound(self, compound):
        with self._path_builder_lock:
            if self.debug:
                print('_build_ramfs_from_compound', compound.compound_name)
            if compound.compound_type == Compound.DIR_TYPE:
                dir_path = compound.compound_name
                self._recreate_imagesaver_dir(dir_path)
                self._internal_ram_fs.makedirs(compound.compound_name, recreate=True)
                assert self._internal_ram_fs.isdir(compound.compound_name)
            elif compound.compound_type == Compound.FILE_TYPE:
                file_path = compound.compound_name
                dir_path = fs.path.dirname(compound.compound_name)
                self._recreate_imagesaver_dir(dir_path)
                self._internal_ram_fs.makedirs(dir_path, recreate=True)
                self._internal_ram_fs.create(file_path)
                assert self._internal_ram_fs.exists(compound.compound_name)
            else:
                pass

    def _unbuild_ramfs_from_compound(self, compound):
        with self._path_builder_lock:
            if self.debug:
                print('_unbuild_ramfs_from_compound', compound.compound_name)
            if compound.compound_type == Compound.DIR_TYPE:
                dir_path = compound.compound_name
                self._internal_ram_fs.removetree(dir_path)
                assert not self._internal_ram_fs.exists(compound.compound_name)
            elif compound.compound_type == Compound.FILE_TYPE:
                file_path = compound.compound_name
                if self._internal_ram_fs.exists(file_path):
                    self._internal_ram_fs.remove(file_path)
                assert not self._internal_ram_fs.exists(compound.compound_name)
            else:
                pass


    def _recreate_imagesaver_dir(self, dir_path):
        with self._path_builder_lock:
            for sub_dir_path in fs.path.recursepath(dir_path):
                if self._internal_ram_fs.exists(sub_dir_path):
                    # print("mem fs has dir", sub_dir_path, "asserting saver also has it, src", dir_path)
                    # assert self._saver.hasCompoundWithName(sub_dir_path)
                    continue
                if not self._saver.hasCompoundWithName(sub_dir_path):
                    abs_path = self._internal_ram_fs.validatepath(sub_dir_path)
                    # print(dir_path, "recreating missing dir", abs_path)
                    self._make_dir_saver(abs_path)

    def flush(self):
        with self._lock:
            self._saver.flushPending()
            self._internal_ram_fs.close()

    def close(self):
        with self._lock:
            self._saver.flush()
            self._internal_ram_fs.close()
            return super().close()

    def getinfo(self, path, namespaces=None):
        with self._lock:
            # abs_path = fs.path.abspath(path)
            abs_path = self._internal_ram_fs.validatepath(path)
            self._build_partial_ramfs_from_praefix(path)
            info = self._internal_ram_fs.getinfo(path, namespaces)
            raw_info = info.raw
            # info.name
            try:
                compound = self._saver.getCompoundWithName(abs_path)
                size = compound.compound_size
            except NotExistingException:
                print('ImageSaverFS.getinfo, unable to get compound', abs_path, 'for size')
                # print(abs_path, path, raw_info)
                return Info(raw_info)
                # raise
            try:
                raw_info['details']['size'] = size
            except KeyError:
                raw_info['details'] = {'size': size}
            if compound.compound_type == Compound.FILE_TYPE:
                raw_info['details']['type'] = fs.enums.ResourceType.file
            elif compound.compound_type == Compound.DIR_TYPE:
                raw_info['details']['type'] = fs.enums.ResourceType.directory

            return Info(raw_info)

    def listdir(self, path):
        with self._lock:
            self._build_partial_ramfs_from_praefix(path)
            return self._internal_ram_fs.listdir(path)

    def makedir(self, path, permissions=None, recreate=False):
        with self._lock:
            self._build_partial_ramfs_from_praefix(path)
            # abs_path = fs.path.abspath(path)
            abs_path = self._internal_ram_fs.validatepath(path)
            self._make_dir_saver(abs_path)
            return self._internal_ram_fs.makedir(path, permissions, recreate)

    def _make_dir_saver(self, abs_path):
        try:
            self._saver.saveBytes(b'', abs_path, compound_type=Compound.DIR_TYPE)
        except CompoundAlreadyExistsException:
            pass

    def openbin(self, path, mode="r", buffering=-1, **options):
        with self._lock:
            self._build_partial_ramfs_from_praefix(path)
            try:
                self._internal_ram_fs.openbin(path, mode, buffering, **options)
            except ResourceNotFound as e:
                print(path, mode, buffering, options)
                print(fs.path.dirname(path))
                print(self._internal_ram_fs.listdir(fs.path.dirname(path)))
                raise
            # abs_path = fs.path.abspath(path)
            abs_path = self._internal_ram_fs.validatepath(path)
            with self._lock:
                # if mode == 'r':
                #     raise NotImplementedError('Only rb mode supported')
                if 'r' in mode and 'w' not in mode:
                    with self._saver:
                        self._internal_ram_fs.openbin(path, mode, buffering, **options)
                        f = FileLikeIterator(self._saver.loadCompound(abs_path))
                        return f
                # elif mode == 'w':
                #     raise NotImplementedError('Only wb mode supported')
                elif 'w' in mode and 'r' not in mode and 'a' not in mode:
                    return self._saver.openWritableCompound(abs_path, compound_type=Compound.FILE_TYPE,
                                                            overwrite=True)
                # elif 'w' in mode and 'r' not in mode and 'a' in mode:
                #     f = self._saver.openWritableCompound(abs_path, compound_type=Compound.FILE_TYPE,
                #                                             overwrite=True, #append=True
                #                                          )
                #     try:
                #         self._internal_ram_fs.openbin(path, mode, buffering, **options)
                #         old_f = FileLikeIterator(self._saver.loadCompound(abs_path))
                #         while True:
                #             chunk = old_f.read(self._saver.fragment_size)
                #             if not chunk:
                #                 break
                #             f.write(chunk)
                #     except ResourceNotFound:
                #         pass
                #     return f
                else:
                    raise NotImplementedError('Unsupported open mode '+repr(mode))

    def remove(self, path):
        with self._lock:
            self._build_partial_ramfs_from_praefix(path)
            self._internal_ram_fs.remove(path)
            # abs_path = fs.path.abspath(path)
            abs_path = self._internal_ram_fs.validatepath(path)
            try:
                self._saver.deleteCompound(abs_path)
            except CompoundNotExistingException:
                pass

    def removedir(self, path):
        with self._lock:
            self._build_partial_ramfs_from_praefix(path)
            self._internal_ram_fs.removedir(path)
            # abs_path = fs.path.abspath(path)
            abs_path = self._internal_ram_fs.validatepath(path)
            self._saver.deleteCompound(abs_path)

    def setinfo(self, path, info):
        with self._lock:
            self._build_partial_ramfs_from_praefix(path)
            self._internal_ram_fs.setinfo(path, info)

    def hash(self, path, name):
        if name.lower() == 'sha256':
            with self._lock:
                self._build_partial_ramfs_from_praefix(path)
                self._internal_ram_fs.hash(path, name)
                abs_path = self._internal_ram_fs.validatepath(path)
                return self._saver.getCompoundWithName(abs_path).compound_hash.hex()
        else:
            return super().hash(path, name)

