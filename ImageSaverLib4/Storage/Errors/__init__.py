class StorageError(Exception):
    pass


class TransferError(StorageError):
    pass


class ManagementError(StorageError):
    pass


class UploadError(TransferError):
    pass


class DownloadError(TransferError):
    pass


class NotFoundError(DownloadError):
    pass


class DeleteError(ManagementError):
    pass


class WipeError(ManagementError):
    pass


class ListError(ManagementError):
    pass
