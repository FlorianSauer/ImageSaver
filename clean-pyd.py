import os


def scandir(dir, files=[]):
    for file in os.listdir(dir):
        path = os.path.join(dir, file)
        if os.path.isfile(path) and (path.endswith(".pyd") or path.endswith(".so")):
            files.append(path)
        elif os.path.isdir(path):
            scandir(path, files)
    return files


def removeFiles(files):
    for file in files:
        print("removing", file)
        os.remove(file)


if __name__ == "__main__":
    removeFiles(scandir("Libs"))

