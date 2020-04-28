class SharedAlbumOptions(object):
    def __init__(self, json_dict):
        self.isCollaborative = json_dict['isCollaborative']  # type: bool
        self.isCommentable = json_dict['isCommentable']  # type: bool
