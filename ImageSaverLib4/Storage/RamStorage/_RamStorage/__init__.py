from typing import Dict, List


class RamStorage(object):
    def __init__(self):
        self.storage = {}  # type: Dict[str, bytes]

    def add(self, name, data):
        # type: (str, bytes) -> None
        self.storage[name] = data

    def load(self, name):
        # type: (str) -> bytes
        return self.storage[name]

    def list(self):
        # type: () -> List[str]
        return list(self.storage.keys())

    def delete(self, name):
        try:
            del self.storage[name]
        except KeyError:
            pass

    def wipe(self):
        self.storage.clear()

    def size(self):
        return sum((len(b) for b in self.storage.values()))
