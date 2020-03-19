from typing import Optional

from .ShareInfo import ShareInfo


class Album(object):
    def __init__(self, json_dict):
        self.id = json_dict['id']  # type: str
        self.title = json_dict['title']  # type: str
        self.productUrl = json_dict['productUrl']  # type: str
        self.isWriteable = json_dict['isWriteable'] if 'isWriteable' in json_dict else None  # type: Optional[bool]
        self.shareInfo = ShareInfo(json_dict['shareInfo']) if 'shareInfo' in json_dict else None  # type: Optional[ShareInfo]
        self.mediaItemsCount = int(json_dict['mediaItemsCount']) if 'mediaItemsCount' in json_dict else 0  # type: int
        self.coverPhotoBaseUrl = json_dict['coverPhotoBaseUrl'] if 'mediaItemsCount' in json_dict else None  # type: Optional[str]
        self.coverPhotoMediaItemId = json_dict['coverPhotoMediaItemId'] if 'coverPhotoMediaItemId' in json_dict else None  # type: Optional[str]
