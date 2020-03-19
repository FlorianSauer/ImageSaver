import fs
from fs.base import FS
from fs.errors import ResourceNotFound
from fs.info import Info
from fs.subfs import SubFS

from ImageSaverLib4.Errors import CompoundAlreadyExistsException, CompoundNotExistingException
from ImageSaverLib4.Helpers.FileLikeIterator import FileLikeIterator
from ImageSaverLib4.ImageSaverLib import ImageSaver
from ImageSaverLib4.MetaDB.Errors import NotExistingException
from ImageSaverLib4.MetaDB.Types.Compound import Compound


class ImageSaverFS(FS):
    def __init__(self, imagesaver):
        # type: (ImageSaver) -> None
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

        self._saver = imagesaver
        try:
            self._saver.saveBytes(b'', '/', compound_type=Compound.DIR_TYPE)
            self._saver.fragment_cache.flush()
            assert self._saver.hasCompoundWithName('/')
        except CompoundAlreadyExistsException:
            pass

    def __enter__(self):
        self._saver.__enter__()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        # print('ImageSaverFS.__exit__', exc_type, exc_value, traceback)
        try:
            self._saver.__exit__(exc_type, exc_value, traceback)
        except Exception as e:
            print(e)
            print(e.__traceback__)
        finally:
            return super().__exit__(exc_type, exc_value, traceback)

    def flush(self):
        with self._lock:
            self._saver.flush()

    def close(self):
        with self._lock:
            self._saver.flush()
            return super().close()

    def getinfo(self, path, namespaces=None):
        with self._lock:
            abs_path = self.validatepath(path)
            # print('getinfo', path, abs_path)
            try:
                compound = self._saver.getCompoundWithName(abs_path)
            except NotExistingException:
                # print('getinfo ERROR', path, abs_path)
                raise ResourceNotFound(abs_path)
            raw_info = {'basic': {'name': fs.path.basename(compound.compound_name),
                                  'is_dir': compound.compound_type == Compound.DIR_TYPE},
                        'details': {'size': compound.compound_size}}
            if compound.compound_type == Compound.FILE_TYPE:
                raw_info['details']['type'] = fs.enums.ResourceType.file
            elif compound.compound_type == Compound.DIR_TYPE:
                raw_info['details']['type'] = fs.enums.ResourceType.directory
            return Info(raw_info)

    def listdir(self, path):
        with self._lock:
            abs_path = self.validatepath(path)
            try:
                compound = self._saver.getCompoundWithName(abs_path)
            except NotExistingException:
                raise fs.errors.ResourceNotFound(abs_path)
            if compound.compound_type != Compound.DIR_TYPE:
                raise fs.errors.DirectoryExpected(abs_path)
            if abs_path == '/':
                slash_count = 1
            else:
                slash_count = abs_path.count('/') + 1
                abs_path += '/'
            compound_gen = self._saver.listCompounds(type_filter=None,
                                                     order_alphabetically=False,
                                                     starting_with=abs_path,
                                                     ending_with=None,
                                                     slash_count=slash_count)
            return [fs.path.basename(c.compound_name) for c in compound_gen if c.compound_name != '/']

    # def validatepath(self, path):
    #     ret = super().validatepath(path)
    #     print('validatepath', path, '->', ret)
    #     return ret

    def makedir(self, path, permissions=None, recreate=False):
        with self._lock:
            abs_path = self.validatepath(path)
            try:
                self._saver.saveBytes(b'', abs_path, compound_type=Compound.DIR_TYPE)
            except CompoundAlreadyExistsException:
                if not recreate:
                    raise fs.errors.DirectoryExists(abs_path)
            return SubFS(self, abs_path)
            return self.opendir(abs_path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        with self._lock:
            abs_path = self.validatepath(path)
            # if mode == 'r':
            #     raise NotImplementedError('Only rb mode supported')
            if 'r' in mode and 'w' not in mode:
                with self._saver:
                    try:
                        compound = self._saver.getCompoundWithName(abs_path)
                    except NotExistingException:
                        raise fs.errors.ResourceNotFound(abs_path)
                    if compound.compound_type != Compound.FILE_TYPE:
                        raise fs.errors.FileExpected(abs_path)
                    f = FileLikeIterator(self._saver.loadCompound(compound.compound_name))
                    return f
            # elif mode == 'w':
            #     raise NotImplementedError('Only wb mode supported')
            elif 'w' in mode and 'r' not in mode and 'a' not in mode:
                if 'x' in mode and self._saver.hasCompoundWithName(abs_path):
                    raise fs.errors.FileExists(abs_path)
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
                raise NotImplementedError('Unsupported open mode ' + repr(mode))

    def remove(self, path):
        with self._lock:
            abs_path = self.validatepath(path)
            try:
                compound = self._saver.getCompoundWithName(abs_path)
            except NotExistingException:
                raise fs.errors.ResourceNotFound(abs_path)
            if compound.compound_type != Compound.FILE_TYPE:
                raise fs.errors.FileExpected(abs_path)
            try:
                self._saver.deleteCompound(abs_path)
            except CompoundNotExistingException:
                pass

    def removedir(self, path):
        with self._lock:
            abs_path = self.validatepath(path)
            if abs_path == '/':
                raise fs.errors.RemoveRootError(abs_path)
            try:
                compound = self._saver.getCompoundWithName(abs_path)
            except NotExistingException:
                raise fs.errors.ResourceNotFound(abs_path)
            else:
                if compound.compound_type != Compound.DIR_TYPE:
                    raise fs.errors.DirectoryExpected(abs_path)

            if len(self.listdir(abs_path)) > 0:
                raise fs.errors.DirectoryNotEmpty(abs_path)
            try:
                self._saver.deleteCompound(abs_path)
            except CompoundNotExistingException:
                pass

    def setinfo(self, path, info):
        raise NotImplementedError("Setting Metadata for a Compound is not supported with ImageSaver")

    def hash(self, path, name):
        # print('ImageSaverFS.hash', path, name)
        if name.lower() == 'sha256':
            with self._lock:
                abs_path = self.validatepath(path)
                try:
                    compound = self._saver.getCompoundWithName(abs_path)
                except NotExistingException:
                    raise fs.errors.ResourceNotFound(abs_path)
                else:
                    return compound.compound_hash.hex()
        else:
            return super().hash(path, name)

    def copy(self, src_path, dst_path, overwrite=False):
        src_path = self.validatepath(src_path)
        dst_path = self.validatepath(dst_path)
        self._saver.copyCompound(src_path, dst_path, overwrite)
