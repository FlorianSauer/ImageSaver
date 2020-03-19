from datetime import datetime


class MediaMetadata(object):
    time_formats = [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M'
    ]

    def __init__(self, json_dict):
        for index, time_format in enumerate(self.time_formats):
            try:
                self.creationTime = datetime.strptime(json_dict['creationTime'][:-4], time_format)  # type: datetime
                break
            except ValueError:
                if index == len(self.time_formats) - 1:
                    raise
        self.width = int(json_dict['width'])  # type: int
        self.height = int(json_dict['height'])  # type: int
        # Todo: add photo and video attributes
        #  https://developers.google.com/photos/library/reference/rest/v1/mediaItems#MediaItem
