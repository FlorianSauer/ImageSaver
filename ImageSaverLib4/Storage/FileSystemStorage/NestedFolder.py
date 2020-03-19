from typing import List

from .Errors import DirFullError


class NestedFolder(object):
    def __init__(self, path, depth, max_depth, max_items, child_number):
        self.sub_folders = []  # type: List[NestedFolder]
        self.sub_files = []  # type: List[int]
        self.free_sub_folders = []  # type: List[NestedFolder]
        self.path = path
        self.depth = depth
        self.max_depth = max_depth
        self.max_items = max_items
        self.child_number = child_number
        self.current_items = 0
        # if self.depth != self.max_depth:
        #     child_number = self.current_items
        #     folder = NestedFolder(self.path + '/' + str(child_number), self.depth + 1, self.max_depth, self.max_items,
        #                           child_number)
        #     self.sub_folders.append(folder)
        #     self.current_items += 1

    def __repr__(self):
        return self.__class__.__name__ + '(' + self.path + ')'

    def getNextName(self):
        # type: () -> List[int]
        # check if folder is at the most inner position
        if self.depth == self.max_depth:
            # check if there is still space left
            if self.current_items >= self.max_items:
                raise DirFullError("Directory " + self.path + " has no more space left")
            else:
                return [self.getNextFileNumber()]
        # otherwise it is a lower nested folder
        else:
            # check free sub folders
            if self.free_sub_folders:
                for folder in list(self.free_sub_folders):
                    try:
                        l = folder.getNextName()
                    except DirFullError:
                        self.free_sub_folders.remove(folder)
                        continue
                    l.append(folder.child_number)
                    return l
            try:
                if not self.sub_folders:
                    raise DirFullError
                folder = self.sub_folders[-1]
                l = folder.getNextName()
                l.append(folder.child_number)
                return l
            except DirFullError:
                if self.current_items >= self.max_items:
                    raise DirFullError("Directory " + self.path + " has no more space left")
                else:
                    child_number = self.getNextSubFolderChildNumber()
                folder = NestedFolder(self.path + '/' + str(child_number), self.depth + 1, self.max_depth,
                                      self.max_items, child_number)
                self.sub_folders.append(folder)
                self.current_items += 1
                l = folder.getNextName()
                l.append(folder.child_number)
                return l

    def getNextSubFolderChildNumber(self):
        int_list = [f.child_number for f in self.sub_folders]
        if len(int_list) == 0:
            return 0
        int_list.sort()
        for index, expected_int in enumerate(range(0, len(int_list))):
            if int_list[index] != expected_int:
                # print "missing int", expected_int, '@', _index
                return expected_int
        return max(int_list) + 1

    def getNextFileNumber(self):
        int_list = [i for i in self.sub_files]
        if len(int_list) == 0:
            return 0
        int_list.sort()
        for index, expected_int in enumerate(range(0, len(int_list))):
            if int_list[index] != expected_int:
                # print "missing int", expected_int, '@', _index
                return expected_int
        return max(int_list)+1

    def getSubFolderWithChildNumber(self, child_number):
        # type: (int) -> NestedFolder
        for folder in self.sub_folders:
            if folder.child_number == child_number:
                return folder

    def useName(self, int_list):
        # type: (List[int]) -> None
        if len(int_list) == 1:  # most inner nested folder
            self.current_items += 1
            self.sub_files.append(int_list[0])
        else:
            folder = self.getSubFolderWithChildNumber(int_list[-1])
            folder.useName(int_list[0:-1])

    def reuse(self, int_list):
        if len(int_list) == 1:  # most inner nested folder
            self.current_items -= 1
            if int_list[0] in self.sub_files:
                self.sub_files.remove(int_list[0])
        else:
            folder = self.getSubFolderWithChildNumber(int_list[-1])
            if not folder:
                if self.current_items == self.max_items:
                    raise DirFullError("Directory " + self.path + " has no more space left")
                else:
                    child_number = int_list[-1]
                folder = NestedFolder(self.path + '/' + str(child_number), self.depth + 1, self.max_depth,
                                      self.max_items, child_number)
                self.sub_folders.append(folder)
                self.current_items += 1
            if folder not in self.free_sub_folders:
                self.free_sub_folders.append(folder)
            folder.reuse(int_list[0:-1])

    def getManagingFolder(self, int_list):
        # type: (List[int]) -> NestedFolder
        if len(int_list) == 0:
            return self
        else:
            folder = self.getSubFolderWithChildNumber(int_list[-1])
            if not folder:
                if self.current_items == self.max_items:
                    raise DirFullError("Directory " + self.path + " has no more space left")
                else:
                    child_number = int_list[-1]
                folder = NestedFolder(self.path + '/' + str(child_number), self.depth + 1, self.max_depth,
                                      self.max_items, child_number)
                self.sub_folders.append(folder)
                self.current_items += 1
                if child_number != self.current_items:
                    self.free_sub_folders.append(folder)
            return folder.getManagingFolder(int_list[0:-1])
