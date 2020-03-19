from .MediaItem import MediaItem
from .Status import Status


class NewMediaItemResult(object):
    def __init__(self, json_dict):
        self.uploadToken = json_dict['uploadToken']  # type: str
        self.status = Status(json_dict['status'])  # type: Status
        self.mediaItem = MediaItem(json_dict['mediaItem'])
