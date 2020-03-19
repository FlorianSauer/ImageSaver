import hashlib
import json
from typing import Optional, Dict


from ImageSaverLib4.Encapsulation.Wrappers.Types import MinimumSizeWrapper, PNG3DWrapper
from ImageSaverLib4.MetaDB.Types.Resource import ResourceSize
from .api.resources.Album import Album
from ..StorageBuilder import StorageBuilderInterface, str_to_bool, str_to_bytesize
from ..StorageInterface import AbstractSizableStorageInterface, StorageSize

"""
Setup

Obtaining a Google Photos API key

1. Obtain a Google Photos API key (Client ID and Client Secret) by following the instructions on 
[Getting started with Google Photos REST APIs](https://developers.google.com/photos/library/guides/get-started)

NOTE 
When selecting your application type in Step 4 of "Request an OAuth 2.0 client ID", 
please select "Other". There's also no need to carry out step 5 in that section.

You then can download a credentials.json, which should be saved in a conveniant location.
Pass this path in the config under the option 'credentials_path'.
During the first startup, a prompt will show up, where you have to grant the created app permission to your google 
photos. After this a second file will be saved under the location given under the config option 'client_token_path'.
"""


class GooglePhotosStorage(AbstractSizableStorageInterface, StorageBuilderInterface):
    """
    This is a Storage Class for the Google Photo API.
    Limits: the google photos api stores uploaded images in original size.
    You have to manually compress them to high quality, to use the unlimited image quota.
    For a free plan this means, that you can upload 15GB of photos per day.

    Internally the GooglePhotosStorage wraps given bytes in an RGB PNG image, so you dont have to do this yourself.
    """
    __storage_name__ = 'gphotos'
    max_album_size = 20000
    _trash_album = 'isl_trash'
    _album_praefix = 'isl_album_'
    required_wrap_type = MinimumSizeWrapper(1000).get_wrapper_type()

    def __init__(self, client_token_path, credentials_path, max_resource_size=None, debug=False, max_storage_size=None):
        # type: (str, str, Optional[ResourceSize], bool, Optional[StorageSize]) -> None
        from google.auth.transport.requests import AuthorizedSession
        AbstractSizableStorageInterface.__init__(self, debug=debug, max_resource_size=max_resource_size, max_storage_size=max_storage_size)
        self.client_token_path = client_token_path
        self.credentials_path = credentials_path
        self._session = None  # type: Optional[AuthorizedSession]
        self._api = None  # type: Optional['ApiCaller']
        self.albums = {}  # type: Dict[str, Album]
        self.albums_count = {}  # type: Dict[str, int]
        self.trash_album = None

    @property
    def session(self):
        if not self._session:
            from .api.ApiCaller import SessionBuilder
            self._session = SessionBuilder.get_authorized_session(self.client_token_path, self.credentials_path)
        return self._session

    @property
    def api(self):
        if not self._api:
            from .api.ApiCaller import ApiCaller
            self._api = ApiCaller(self.session)
        return self._api

    def _calculateCurrentSize(self):
        size = 0
        for album in self.api.listAlbums(exclude_non_app_created=True):
            for mediaitem in self.api.listMediaItemsInAlbum(album):
                size += self.api.getSizeOfMediaItem(mediaitem)
        return size

    @classmethod
    def build(cls, client_token_path, credentials_path, debug='False', max_resource_size=None, max_storage_size=None):
        debug = str_to_bool(debug)
        if max_resource_size:
            max_resource_size = str_to_bytesize(max_resource_size)
        if max_storage_size:
            max_storage_size = str_to_bytesize(max_storage_size)
        return cls(client_token_path, credentials_path, max_resource_size, debug, max_storage_size)

    def identifier(self):
        from .api.ApiCaller import SessionBuilder
        return hashlib.sha256(SessionBuilder.get_cred_json(self.session.credentials).encode('utf-8')).hexdigest()

    def loadRessource(self, resource_name):
        src_album_id, mediaitem_id = self.parse_resource_name(resource_name)
        media_item = self.api.getMediaItemByID(mediaitem_id)
        remote_png = self.api.downloadMediaItem(media_item)
        remote_png_payload = PNG3DWrapper.unwrap(remote_png)
        return remote_png_payload

    def nextEmptyAlbum(self):
        # type: () -> Album
        if len(self.albums) == 0:
            # either no albums created or not yet fetched
            # first try fetching
            for a in self.api.listAlbums(exclude_non_app_created=True):
                if a.title.startswith(self._album_praefix) and a.title.replace(self._album_praefix, '').isnumeric():
                    self.albums_count[a.id] = a.mediaItemsCount
                    self.albums[a.id] = a

        # if albums dict is empty, create a new album
        if len(self.albums) == 0:
            print("creating storage album", self._album_praefix+'0')
            a = self.api.createAlbum(self._album_praefix+'0')
            self.albums_count[a.id] = a.mediaItemsCount
            self.albums[a.id] = a
            return a
        # otherwise search biggest album where size is smaller than 20000
        else:
            if any((c < self.max_album_size for c in self.albums_count.values())):
                # album exists where size is smaller than 20000
                biggest_album_title = sorted(self.albums_count.keys(),
                                             key=lambda k: self.albums_count[k],
                                             reverse=True)[0]
                return self.albums[biggest_album_title]
            else:
                # all albums are maxed out to 20000, create a new one
                print("creating storage album", self._album_praefix + str(len(self.albums)))
                a = self.api.createAlbum(self._album_praefix + str(len(self.albums)))
                self.albums_count[a.id] = a.mediaItemsCount
                self.albums[a.id] = a
                return a

    def saveResource(self, resource_data, resource_hash, resource_size):
        png = PNG3DWrapper.wrap(resource_data)
        album = self.nextEmptyAlbum()
        file_name = resource_hash.hex() + '.png'
        token = self.api.uploadBytes(file_name, png)
        new_media_item = self.api.createMediaItem([(token, file_name), ], album)[0]
        try:
            self.albums_count[album.id] += 1
        except KeyError:
            print(self.albums_count)
            print(self.albums)
            raise
        self.increaseCurrentSize(resource_size)
        return self.format_resource_name(album.id, new_media_item.mediaItem.id)

    def createTrashAlbum(self):
        if not self.trash_album:
            for album in self.api.listAlbums(exclude_non_app_created=True):
                if album.title == self._trash_album:
                    self.trash_album = album
                    return self.trash_album
            print("creating trash album")
            self.trash_album = self.api.createAlbum(self._trash_album)
            return self.trash_album
        else:
            return self.trash_album

    def deleteResource(self, resource_name):
        self.resetCurrentSize()
        src_album_id, mediaitem_id = self.parse_resource_name(resource_name)
        # print('deleteResource', src_album_id, mediaitem_id)
        src_album = self.api.getAlbumByID(src_album_id)
        trash_album = self.createTrashAlbum()
        media_item = self.api.getMediaItemByID(mediaitem_id)
        self.api.addMediaItemsToAlbum(trash_album, [media_item, ])
        self.api.removeMediaItemsFromAlbum(src_album, [media_item, ])

    def listResourceNames(self):
        id_list = []
        for album in self.api.listAlbums(exclude_non_app_created=True):
            if album.title == self._trash_album:
                continue
            for media_item in self.api.listMediaItemsInAlbum(album):
                id_list.append(self.format_resource_name(album.id, media_item.id))
        return id_list

    def wipeResources(self):
        self.resetCurrentSize()
        trash_album = None
        for album in self.api.listAlbums(exclude_non_app_created=True):
            # print(list(self.api.listMediaItemsInAlbum(album)))
            album_media_items = list(self.api.listMediaItemsInAlbum(album))
            if len(album_media_items) > 0:
                if not trash_album:
                    trash_album = self.createTrashAlbum()
                self.api.addMediaItemsToAlbum(trash_album, album_media_items)
                self.api.removeMediaItemsFromAlbum(album, album_media_items)

    def parse_resource_name(self, resource_name):
        j = json.loads(resource_name)
        return j['aid'], j['mid']

    def format_resource_name(self, album_id, mediaitem_id):
        return json.dumps({'aid': album_id, 'mid': mediaitem_id}, sort_keys=True)
