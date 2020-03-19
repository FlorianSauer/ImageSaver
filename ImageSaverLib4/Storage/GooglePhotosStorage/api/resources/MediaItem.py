from typing import Optional

from .ContributorInfo import ContributorInfo
from .MediaMetadata import MediaMetadata


class MediaItem(object):
    def __init__(self, json_dict):
        self.id = json_dict['id']  # type: str
        self.description = json_dict['description'] if 'description' in json_dict else None  # type: Optional[str]
        self.productUrl = json_dict['productUrl']  # type: str
        self.baseUrl = json_dict['baseUrl'] if 'baseUrl' in json_dict else None  # type: Optional[str]
        self.mimeType = json_dict['mimeType']  # type: str
        self.mediaMetadata = MediaMetadata(json_dict['mediaMetadata'])  # type: MediaMetadata
        self.contributorInfo = ContributorInfo(
            json_dict['contributorInfo']) if 'contributorInfo' in json_dict else None  # type: Optional[ContributorInfo]
        self.filename = json_dict['filename']  # type: str
