import hashlib
from typing import List, Optional

from .Connectors import FileSystemInterface
from .Errors import DirStructureError
from .NestedFolder import NestedFolder


class FolderStructurizer(object):
    def __init__(self, connector, root='.', extension='bin', folder_depth=2, folder_max_items=1000):
        # type: (FileSystemInterface, str, str, int, int) -> None
        # assert folder_depth >= 1
        self.fs_connector = connector
        self.root = root.replace('\\', '/')

        self.extension = extension
        self.folder_depth = folder_depth
        self.folder_max_items = folder_max_items

        self.nested_folders = None  # type: Optional[NestedFolder]

        # self.build_current_pool()

    def build_current_pool(self):
        """
        walk through root and check where space can be reused
        """
        self.nested_folders = NestedFolder(self.root, 0, self.folder_depth, self.folder_max_items, 0)
        self.fs_connector.os_makedirs(self.root)
        for dirname, folders, files in self.fs_connector.os_walk(self.root):  # type: str, List[str], List[str]
            dirname = dirname.replace('\\', '/')
            if dirname == self.root:
                dirname_noroot = '/'
                current_depth = 0
            else:
                dirname_noroot = dirname.replace(self.root, '', 1)
                if not dirname_noroot.startswith('/'):
                    dirname_noroot = '/'+dirname_noroot
                current_depth = dirname_noroot.count('/')
            # ####
            # if dirname == self.root:
            #     dirname_noroot = ''  # type: str
            #
            #     current_depth = 1
            # else:
            #     dirname_noroot = dirname.replace(self.root, '', 1)  # type: str
            #     # dirname = str(PurePosixPath(Path(dirname)))
            #
            #     current_depth = dirname_noroot.count('/')
            #
            # # if dirname.startswith('/'):
            # #     current_depth += 1
            # if dirname_noroot == '':
            #     dirname_noroot = '/'
            # elif dirname_noroot[0] == '/':
            #     dirname_noroot = dirname_noroot[1:]
            is_valid_dirname = all((n.isnumeric() for n in dirname_noroot.split('/') if n))
            if not is_valid_dirname:
                raise DirStructureError(
                    "Directory " + repr(dirname) + " has an unsupported folder name, only a numeric name is allowed")
            if current_depth == self.folder_depth:
                if len(folders) > 0:
                    raise DirStructureError(
                        "Directory " + repr(dirname) + " contains folders, no folders allowed on depth " + str(
                            current_depth))
                # if not all((f.rsplit('.', 1)[0].isnumeric() for f in files)):
                #     raise DirStructureError(
                #         "Directory " + repr(dirname) + " has an unsupported file name, only a numeric name is allowed")
                int_list = [int(n) for n in dirname_noroot.split('/') if n]
                if int_list and max(int_list) >= self.folder_max_items:
                    raise DirStructureError(
                        "Directory " + repr(
                            dirname) + " has an unsupported directory name, only directory names with a name between 0 and " + str(
                            self.folder_max_items - 1) + " are allowed")

                int_list.reverse()
                self.nested_folders.reuse([0] + int_list)
                folder = self.nested_folders.getManagingFolder(int_list)
                folder.current_items = len(files)
                # folder.sub_files = [int(f.rsplit('.', 1)[0]) for f in files]
                folder.sub_files = list(range(len(files)))
                # if folder.current_items >= self.folder_max_items:
                #     raise DirStructureError(
                #         "Directory " + repr(
                #             dirname) + " has an unsupported file name, only filenames with a name between 0 and " + str(
                #             self.folder_max_items - 1) + " are allowed")
            else:
                if len(files) > 0:
                    raise DirStructureError(
                        "Directory " + repr(dirname) + " contains files, no files allowed on depth " + str(
                            current_depth))

    def add(self, data, data_hash, data_size):
        # type: (bytes, bytes, int) -> str
        if not self.nested_folders:
            self.build_current_pool()
        pool_path = self.nested_folders.getNextName()
        # print(pool_path)
        pool_path_str = '/'.join((str(i) for i in reversed(pool_path[1:])))
        file_name = self.makeDataName(pool_path, data_hash, data_size)
        path = self.fs_connector.path_join(self.root, pool_path_str)
        data_name = self.fs_connector.path_join(pool_path_str, file_name + '.' + self.extension)
        self.fs_connector.os_makedirs(path)
        path = self.fs_connector.path_join(path, file_name + '.' + self.extension)
        # print(path)
        self.fs_connector.saveFile(data, path)
        self.nested_folders.useName(pool_path)
        return data_name

    def makeDataName(self, pool_path, data_hash, data_size):
        # type: (List[int], bytes, int) -> str
        h = hashlib.sha256(
            '/'.join((str(i) for i in pool_path)).encode('ascii')
            +
            data_hash
        ).hexdigest()
        return h
        # return str(pool_path[0])

    def get(self, data_name):
        # type: (str) -> bytes
        if not self.nested_folders:
            self.build_current_pool()
        # path = self.fs_connector.path_join(self.root, data_name + '.' + self.extension)
        path = self.fs_connector.path_join(self.root, data_name)
        return self.fs_connector.loadFile(path)

    def delete(self, data_name):
        # type: (str) -> None
        if not self.nested_folders:
            self.build_current_pool()
        # path = self.fs_connector.path_join(self.root, data_name + '.' + self.extension)
        path = self.fs_connector.path_join(self.root, data_name)
        self.fs_connector.deleteFile(path)
        pool_path = data_name.rsplit('/', 1)[0]
        # print(pool_path)
        pool_path = list((int(i) for i in pool_path.split('/')))
        pool_path.reverse()
        self.nested_folders.reuse([0] + pool_path)

    def list(self):
        if not self.nested_folders:
            self.build_current_pool()
        all_files = []
        for dirname, folders, files in self.fs_connector.os_walk(self.root):
            if files:
                dirname_noroot = dirname.replace(self.root, '', 1)  # type: str
                if dirname_noroot == '':
                    dirname_noroot = '/'
                dirname_noroot = dirname_noroot.replace('\\', '/')
                if dirname_noroot[0] == '/':
                    dirname_noroot = dirname_noroot[1:]
                for file in files:
                    all_files.append(
                        # self.fs_connector.path_join(dirname_noroot, self._rreplace(file, '.' + self.extension, '', 1))
                        self.fs_connector.path_join(dirname_noroot, file)
                    )
        return all_files

    def wipe(self):
        self.fs_connector.os_rmdir(self.root)
        self.fs_connector.os_makedirs(self.root)
        self.build_current_pool()

    @staticmethod
    def _rreplace(string, old, new, count=None):
        # type: (str, str, str, Optional[int]) -> str
        if count:
            return new.join(string.rsplit(old, count))
        else:
            return new.join(string.rsplit(old))
