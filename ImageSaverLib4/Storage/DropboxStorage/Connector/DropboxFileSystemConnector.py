import hashlib

from ...FileSystemStorage import FileSystemInterface


class DropboxFileSystemConnector(FileSystemInterface):

    def __init__(self, access_token):
        import dropbox
        self.access_token = access_token
        self.client = dropbox.Dropbox(self.access_token, timeout=60)

    def identifier(self):
        return "<Dropbox_"+hashlib.sha256(self.access_token).hexdigest+">"

    def os_makedirs(self, path):
        from dropbox.exceptions import ApiError
        from dropbox.files import CreateFolderError

        if path != '/':
            path = path[1:] if path.startswith('.') else path
            try:
                self.client.files_create_folder(path, )
            except ApiError as e:
                if type(e.error) is CreateFolderError:
                    pass
                else:
                    raise

    def os_rmdir(self, path):
        if path == '/' or path == '.':
            folder_items = self.client.files_list_folder('').entries
            for folder_item in folder_items:
                self.client.files_delete('/'+folder_item.name)
        else:
            self.client.files_delete(path)

    def saveFile(self, data, path):
        from dropbox import files
        # print(self, "uploading", len(data), "bytes")
        self.client.files_upload(data, path, mode=files.WriteMode.overwrite)
        return True

    def loadFile(self, path):
        return bytes(self.client.files_download(path)[1].content)

    def deleteFile(self, path):
        self.client.files_delete(path)

    def os_walk(self, path):
        from dropbox.files import FolderMetadata, FileMetadata
        dirs, nondirs = [], []

        path = '' if path == '/' or path == '.' else path
        folder_items = self.client.files_list_folder(path).entries

        for folder_item in folder_items:
            if type(folder_item) == FolderMetadata:
                dirs.append(folder_item.name)
            elif type(folder_item) == FileMetadata:
                nondirs.append(folder_item.name)

        # path = '.' if path == '' else path
        if path == '':
            path = '/'
        # dirs = ['./'+d for d in dirs]
        # else:
            # if path.startswith('/'):
            #     path = path.replace('/', '', 1)
        yield path, dirs, nondirs

        for folder_item in dirs:
            new_path = self.path_join(path, folder_item)
            for x in self.os_walk(new_path):
                yield x

    def fileSize(self, path):
        # noinspection PyUnresolvedReferences
        self.client.files_get_metadata(path).size

    def totalSize(self):
        usage = self.client.users_get_space_usage()
        return usage.allocation.get_individual().allocated

    def currentSize(self):
        usage = self.client.users_get_space_usage()
        return usage.used

    def remaining_size(self):
        usage = self.client.users_get_space_usage()
        return usage.allocation.get_individual().allocated - usage.used

