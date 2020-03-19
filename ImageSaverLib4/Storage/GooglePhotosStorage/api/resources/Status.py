from typing import List


class Status(object):
    def __init__(self, json_dict):
        self.code = int(json_dict['code']) if 'code' in json_dict else -1  # type: int
        self.message = json_dict['message']  # type: str
        self.details = json_dict['details'] if 'details' in json_dict else []  # type: List[dict]
