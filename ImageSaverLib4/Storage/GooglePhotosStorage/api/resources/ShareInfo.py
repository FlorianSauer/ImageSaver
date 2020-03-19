from .SharedAlbumOptions import SharedAlbumOptions


class ShareInfo(object):
    def __init__(self, json_dict):
        self.sharedAlbumOptions = SharedAlbumOptions(json_dict['sharedAlbumOptions'])  # type: SharedAlbumOptions
        self.shareableUrl = json_dict['shareableUrl']  # type: str
        self.shareToken = json_dict['shareToken']  # type: str
        self.isJoined = json_dict['isJoined']  # type: bool
        self.isOwned = json_dict['isOwned']  # type: bool
