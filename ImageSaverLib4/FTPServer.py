"""
fs.expose.ftp
==============

Expose an FS object over FTP (via pyftpdlib).

This module provides the necessary interfaces to expose an FS object over
FTP, plugging into the infrastructure provided by the 'pyftpdlib' module.

To use this in combination with fsserve, do the following:

$ fsserve -t 'ftp' $HOME

The above will serve your home directory in read-only mode via anonymous FTP on the
loopback address.
"""

import errno
import os
import stat
import sys
import time
from functools import wraps
from typing import cast

import fs
from fs import errors as fs_errors
from fs.base import FS
from fs.iotools import make_stream
from fs.path import *
# Get these once so we can reuse them:
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.filesystems import AbstractedFS
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import ThreadedFTPServer

from ImageSaverLib4.ImageSaverFS2 import ImageSaverFS

if sys.platform == "win32":
    UID = 1
    GID = 1
else:
    UID = os.getuid()
    GID = os.getgid()


def convert_fs_errors(func):
    """Function wrapper to convert FSError instances into OSError."""

    @wraps(func)
    def wrapper(*args, **kwds):
        try:
            return func(*args, **kwds)
        except fs_errors.ResourceNotFound as e:
            raise OSError(errno.ENOENT, str(e))
        # except ParentDirectoryMissingError as e:
        #     if sys.platform == "win32":
        #         raise OSError(errno.ESRCH,str(e))
        #     else:
        #         raise OSError(errno.ENOENT,str(e))
        except fs_errors.ResourceInvalid as e:
            raise OSError(errno.EINVAL, str(e))
        except fs_errors.PermissionDenied as e:
            raise OSError(errno.EACCES, str(e))
        except fs_errors.ResourceLocked as e:
            if sys.platform == "win32":
                raise WindowsError(32, str(e))
            else:
                raise OSError(errno.EACCES, str(e))
        except fs_errors.DirectoryNotEmpty as e:
            raise OSError(errno.ENOTEMPTY, str(e))
        except fs_errors.DestinationExists as e:
            raise OSError(errno.EEXIST, str(e))
        except fs_errors.InsufficientStorage as e:
            raise OSError(errno.ENOSPC, str(e))
        except fs_errors.RemoteConnectionError as e:
            raise OSError(errno.ENETDOWN, str(e))
        except fs_errors.Unsupported as e:
            raise OSError(errno.ENOSYS, str(e))
        except fs_errors.FSError as e:
            raise OSError(errno.EFAULT, str(e))

    return wrapper


def filelike_to_stream(f):
    @wraps(f)
    def wrapper(self, path, mode='rt', buffering=-1, encoding=None, errors=None, newline=None, line_buffering=False,
                **kwargs):
        file_like = f(self,
                      path,
                      mode=mode,
                      buffering=buffering,
                      encoding=encoding,
                      errors=errors,
                      newline=newline,
                      line_buffering=line_buffering,
                      **kwargs)
        return make_stream(path,
                           file_like,
                           mode=mode,
                           buffering=buffering,
                           encoding=encoding,
                           errors=errors,
                           newline=newline,
                           line_buffering=line_buffering)

    return wrapper


def fix_path(path, working_dir=None):
    # print('fix_path', path)
    path = path.replace('\\', '/')
    if working_dir:
        if working_dir == '/' and path == '..':
            path = '/'
        else:
            path = fs.path.join(working_dir, path)
    path = fs.path.normpath(path)
    return path


def fix_paths_dec(f):
    @wraps(f)
    def wrapper(self, *args):
        return f(self, *args)
        # corrected = []
        # for arg in args:
        #     corrected.append(fix_path(arg))
        # return f(self, *corrected)

    return wrapper


def decode_args(f):
    """
    Decodes string arguments using the decoding defined on the method's class.
    This decorator is for use on methods (functions which take a class or instance
    as the first parameter).

    Pyftpdlib (as of 0.7.0) uses str internally, so this decoding is necessary.
    """

    @wraps(f)
    def wrapper(self, *args):
        encoded = []
        for arg in args:
            if isinstance(arg, bytes):
                # print('decode_args', args)
                arg = arg.decode(self.encoding)
            encoded.append(arg)
        return f(self, *encoded)

    return wrapper


class FakeStat(object):
    """
    Pyftpdlib uses stat inside the library. This class emulates the standard
    os.stat_result class to make pyftpdlib happy. Think of it as a stat-like
    object ;-).
    """

    def __init__(self, **kwargs):
        for attr in dir(stat):
            if not attr.startswith('ST_'):
                continue
            attr = attr.lower()
            value = kwargs.get(attr, 0)
            setattr(self, attr, value)


class FTPFS(AbstractedFS):
    """
    The basic FTP Filesystem. This is a bridge between a pyfs filesystem and pyftpdlib's
    AbstractedFS. This class will cause the FTP server to serve the given fs instance.
    """
    encoding = 'utf8'
    "Sets the encoding to use for paths."

    def __init__(self, fs_obj, root, cmd_channel, encoding=None):
        # type: (FS, str, object, object) -> None
        self.fs = fs_obj
        if encoding is not None:
            self.encoding = encoding
        super(FTPFS, self).__init__(root, cmd_channel)

    def close(self):
        print('FTPFS.close')
        if isinstance(self.fs, ImageSaverFS):
            cast(ImageSaverFS, self.fs).flush()
        else:
            self.fs.close()

    @fix_paths_dec
    def validpath(self, path):
        path = fix_path(path, self._cwd)
        # noinspection PyBroadException
        try:
            normpath(path)
            return True
        except Exception:
            return False

    @convert_fs_errors
    @decode_args
    @filelike_to_stream
    def open(self, path, mode, **kwargs):
        path = fix_path(path, self._cwd)
        print('FTPFS.open', path, mode, kwargs)
        return self.fs.open(path, mode, **kwargs)

    @convert_fs_errors
    @fix_paths_dec
    def chdir(self, path):
        print('FTPFS.chdir', path)
        path = fix_path(path, self._cwd)
        # We dont' use the decorator here, we actually decode a version of the
        # path for use with pyfs, but keep the original for use with pyftpdlib.
        # if not isinstance(path, str):
        #     # pyftpdlib 0.7.x
        #     unipath = str(path, self.encoding)
        # else:
        #     # pyftpdlib 1.x
        unipath = path
        # TODO: can the following conditional checks be farmed out to the fs?
        # If we don't raise an error here for files, then the FTP server will
        # happily allow the client to CWD into a file. We really only want to
        # allow that for directories.
        if self.fs.isfile(unipath):
            raise OSError(errno.ENOTDIR, 'Not a directory')
        # similarly, if we don't check for existence, the FTP server will allow
        # the client to CWD into a non-existent directory.
        if not self.fs.exists(unipath):
            raise OSError(errno.ENOENT, 'Does not exist')
        # We use the original path here, so we don't corrupt self._cwd
        self._cwd = self.ftp2fs(path)

    @fix_paths_dec
    def ftp2fs(self, ftppath):
        p = fix_path(ftppath, self._cwd)
        # p = fs.path.join(self._cwd, ftppath)
        return p
        # return fs.path.normpath(fs.path.join(self.root, p))
        # print('FTPFS.ftp2fs', ftppath)
        # return fs.path.abspath(ftppath)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def mkdir(self, path):
        path = fix_path(path, self._cwd)
        print('FTPFS.mkdir', path)
        self.fs.makedir(path)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def listdir(self, path):
        path = fix_path(path, self._cwd)
        print('FTPFS.listdir', path)
        # print(self.fs.listdir(path))
        return self.fs.listdir(path)
        # return map(lambda x: x.encode(self.encoding), self.fs.listdir(path))

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def rmdir(self, path):
        path = fix_path(path, self._cwd)
        print('FTPFS.rmdir', path)
        self.fs.removedir(path)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def remove(self, path):
        path = fix_path(path, self._cwd)
        print('FTPFS.remove', path)
        self.fs.remove(path)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def rename(self, src, dst):
        print('FTPFS.rename', src, dst)
        src = fix_path(src, self._cwd)
        dst = fix_path(dst, self._cwd)
        self.fs.rename(src, dst)

    @convert_fs_errors
    @decode_args
    def chmod(self, path, mode):
        return

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def stat(self, path):
        print('FTPFS.stat', path)
        path = fix_path(path, self._cwd)
        info = self.fs.getinfo(path)
        kwargs = {'st_size': info.size,
                  'st_uid': UID,
                  'st_gid': GID}  # type: dict
        # Give the fs a chance to provide the uid/gid. Otherwise echo the current
        # uid/gid.
        if info.accessed:
            kwargs['st_atime'] = time.mktime(info.accessed.timetuple())
        if info.created:
            kwargs['st_mtime'] = time.mktime(info.created.timetuple())
            # Pyftpdlib uses st_ctime on Windows platform, try to provide it.
            kwargs['st_ctime'] = kwargs['st_mtime']
        # Try to use existing mode.
        # kwargs['st_mode'] = info.permissions.mode
        # else:
        # Otherwise, build one. Not executable by default.
        mode = 0o0660
        # Merge in the type (dir or file). File is tested first, some file systems
        # such as ArchiveMountFS treat archive files as directories too. By checking
        # file first, any such files will be only files (not directories).
        if self.fs.isfile(path):
            mode |= stat.S_IFREG
        elif self.fs.isdir(path):
            mode |= stat.S_IFDIR
            mode |= 0o0110  # Merge in exec bit to signal dir is listable
        kwargs['st_mode'] = mode
        return FakeStat(**kwargs)

    # No link support...
    lstat = stat

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def isfile(self, path):
        print('FTPFS.isfile', path)
        path = fix_path(path, self._cwd)
        return self.fs.isfile(path)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def isdir(self, path):
        path = fix_path(path, self._cwd)
        print('FTPFS.isdir', path, self.fs.isdir(path))
        return self.fs.isdir(path)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def getsize(self, path):
        print('FTPFS.getsize', path)
        path = fix_path(path, self._cwd)
        return self.fs.getsize(path)

    @convert_fs_errors
    @decode_args
    @fix_paths_dec
    def getmtime(self, path):
        print('FTPFS.getmtime', path)
        path = fix_path(path, self._cwd)
        return self.stat(path).st_mtime

    @fix_paths_dec
    def realpath(self, path):
        print('FTPFS.realpath', path)
        path = fix_path(path, self._cwd)
        return path

    @fix_paths_dec
    def lexists(self, path):
        return True


class FTPFSHandler(FTPHandler):
    """
    An FTPHandler class that closes the filesystem when done.
    """

    def _on_dtp_connection(self):
        super()._on_dtp_connection()

    def close(self):
        # Close the FTPFS instance, it will close the pyfs file system.
        if self.fs:
            print('closing fs')
            self.fs.close()
        super(FTPFSHandler, self).close()

    def initiate_send(self):
        super().initiate_send()


class FTPFSFactory(object):
    """
    A factory class which can hold a reference to a file system object and
    encoding, then later pass it along to an FTPFS instance. An instance of
    this object allows multiple FTPFS instances to be created by pyftpdlib
    while sharing the same fs.
    """

    def __init__(self, fs, encoding=None):
        """
        Initializes the factory with an fs instance.
        """
        self.fs = fs
        self.encoding = encoding

    def __call__(self, root, cmd_channel):
        """
        This is the entry point of pyftpdlib. We will pass along the two parameters
        as well as the previously provided fs instance and encoding.
        """
        return FTPFS(self.fs, root, cmd_channel, encoding=self.encoding)


# class HomeFTPFS(FTPFS):
#     """
#     A file system which serves a user's home directory.
#     """
#     def __init__(self, root, cmd_channel):
#         """
#         Use the provided user's home directory to create an FTPFS that serves an OSFS
#         rooted at the home directory.
#         """
#         super(DemoFS, self).__init__(OSFS(root_path=root), '/', cmd_channel)


def serve_fs(fs, addr, port):
    """
    Creates a basic anonymous FTP server serving the given FS on the given address/port
    combo.
    """
    ftp_handler = FTPFSHandler
    ftp_handler.authorizer = DummyAuthorizer()
    ftp_handler.use_sendfile = False
    ftp_handler.authorizer.add_anonymous('/', perm='elrdmw')
    ftp_handler.abstracted_fs = FTPFSFactory(fs)
    s = ThreadedFTPServer((addr, port), ftp_handler)
    s.serve_forever()
