import time
from io import BytesIO
from typing import Optional


from ...FileSystemStorage import FileSystemInterface


class SambaFileSystemConnector(FileSystemInterface):

    def __init__(self, user_id, password, server_ip, server_name=None, client_machine_name='imagesaver',
                 service_name='ImageSaver', ping_interval=15.0, debug=False):
        # type: (str, str, str, Optional[str], str, str, float, bool) -> None
        self.user_id = user_id
        self.password = password
        self.server_ip = server_ip
        if server_name:
            self.server_name = server_name
        else:
            self.server_name = self.server_ip
        self.client_machine_name = client_machine_name
        self.service_name = service_name
        self._client = None
        self._last_client_action = 0.0
        self._ping_interval = ping_interval
        self.__debug = debug

    def debug(self, *args):
        if self.__debug:
            s = []
            for a in args:
                if type(a) in (float, int):
                    s.append(str(a))
                elif type(a) is str:
                    s.append(a)
                else:
                    s.append(repr(a))
            print(' '.join(s))

    def identifier(self):
        return '_'.join((self.__class__.__name__, self.user_id, self.server_ip, self.service_name))

    @property
    def client(self):
        from smb.SMBConnection import SMBConnection
        from smb.base import NotConnectedError

        now = time.time()
        if self._last_client_action + self._ping_interval < now:
            try:
                if not self._client:
                    raise NotConnectedError
                self.debug('pinging server')
                self._client.echo(b'ping')
            except NotConnectedError:
                self.debug('recreating connection')
                self._client = SMBConnection(self.user_id, self.password, self.client_machine_name, self.server_name,
                                             use_ntlm_v2=True,
                                             is_direct_tcp=True)
                self._client.connect(self.server_ip, 445)
        self._last_client_action = time.time()
        return self._client

    def os_makedirs(self, path):
        from smb.smb_structs import OperationFailure

        folders = [f for f in path.split('/') if f]
        path_depth = len(folders)
        created_path = ''
        for depth, folder in enumerate(folders):
            created_path += '/' + folder
            # print("created path", created_path)
            try:
                self.debug('creating direactory', created_path)
                self.client.createDirectory(self.service_name, created_path)
            except OperationFailure:
                if not self._dir_exists(path) and depth == path_depth - 1:
                    raise

    def _dir_exists(self, path):
        from smb.base import SharedFile
        from smb.smb_structs import OperationFailure

        try:
            self.debug('checking dir exists', path)
            a = self.client.getAttributes(self.service_name, path)  # type: SharedFile
            return a.isDirectory
        except OperationFailure:
            return False

    def _file_exists(self, path):
        from smb.base import SharedFile
        from smb.smb_structs import OperationFailure

        try:
            self.debug('checking file exists', path)
            a = self.client.getAttributes(self.service_name, path)  # type: SharedFile
            return not a.isDirectory
        except OperationFailure:
            return False

    def _delete_foldercontent_recursively(self, path):
        # print('Walking path', path)
        self.debug('listing contents of path', path)
        for p in self.client.listPath(self.service_name, path):
            if p.filename != '.' and p.filename != '..':
                parentPath = path
                if not parentPath.endswith('/'):
                    parentPath += '/'

                if p.isDirectory:
                    self._delete_foldercontent_recursively(parentPath + p.filename)
                    # print('Deleting folder (%s) in %s' % (p.filename, path))
                    self.debug('deleting directory', parentPath + p.filename)
                    self.client.deleteDirectory(self.service_name, parentPath + p.filename)
                else:
                    # print('Deleting file (%s) in %s' % (p.filename, path))
                    self.debug('deleting files', parentPath + p.filename)
                    self.client.deleteFiles(self.service_name, parentPath + p.filename)

    def os_rmdir(self, path):
        if self._dir_exists(path):
            self._delete_foldercontent_recursively(path)
            # self.client.deleteFiles(self.service_name, path+'/*')
            self.debug('deleting directory', path)
            self.client.deleteDirectory(self.service_name, path)
        # else:
        #     raise FileNotFoundError

    def saveFile(self, data, path):
        file_obj = BytesIO(data)
        self.debug('saving file', path)
        self.client.storeFile(self.service_name, path, file_obj)

    def loadFile(self, path):
        file_obj = BytesIO()
        self.debug('loading file', path)
        self.client.retrieveFile(self.service_name, path, file_obj)
        file_obj.seek(0)
        return bytes(file_obj.read())

    def deleteFile(self, path):
        if self._file_exists(path):
            self.debug('deleting files', path)
            self.client.deleteFiles(self.service_name, path)
        # else:
        #     raise FileNotFoundError(repr(path))

    def os_walk(self, path):
        dirs, nondirs = [], []

        self.debug('listing contents of path', path)
        names = self.client.listPath(self.service_name, path)

        for name in names:
            if name.isDirectory:
                if name.filename not in ['.', '..']:
                    dirs.append(name.filename)
            else:
                nondirs.append(name.filename)

        yield path, dirs, nondirs

        for name in dirs:
            new_path = self.path_join(path, name)
            for x in self.os_walk(new_path):
                yield x

    def fileSize(self, path):
        return self.client.getAttributes(self.service_name, path).file_size
