class ContributorInfo(object):
    def __init__(self, json_dict):
        self.profilePictureBaseUrl = json_dict['profilePictureBaseUrl']  # type: str
        self.displayName = json_dict['displayName']  # type: str
