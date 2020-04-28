import json
import logging
import os
from typing import Generator, List, Tuple

# noinspection PyPackageRequirements
from google.auth.transport.requests import AuthorizedSession
# noinspection PyPackageRequirements
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ImageSaverLib.Helpers import chunkiterable_gen
from ImageSaverLib.Storage.Errors import UploadError, DownloadError, StorageError, NotFoundError
from .resources.Album import Album
from .resources.MediaItem import MediaItem
from .resources.NewMediaItemResult import NewMediaItemResult


# inspired by gphotos-upload
# source https://github.com/eshmu/gphotos-upload


class SessionBuilder(object):
    @classmethod
    def _auth(cls, scopes, credentials_file):
        flow = InstalledAppFlow.from_client_secrets_file(
            credentials_file,
            scopes=scopes)

        credentials = flow.run_local_server(host='localhost',
                                            port=8080,
                                            authorization_prompt_message="",
                                            success_message='The auth flow is complete; you may close this window.',
                                            open_browser=True)

        return credentials

    @classmethod
    def get_authorized_session(cls, auth_token_file, credentials_file):
        scopes = ['https://www.googleapis.com/auth/photoslibrary',
                  'https://www.googleapis.com/auth/photoslibrary.sharing']

        credentials_file = os.path.abspath(os.path.expanduser(credentials_file))
        if not os.path.exists(credentials_file):
            raise ValueError('credentials file missing')

        auth_token_file = os.path.abspath(os.path.expanduser(auth_token_file))

        if os.path.exists(auth_token_file):
            try:
                cred = Credentials.from_authorized_user_file(auth_token_file, scopes)
                return AuthorizedSession(cred)
            except OSError as err:
                logging.debug("Error opening auth token file - {0}".format(err))
            except ValueError:
                logging.debug("Error loading auth tokens - Incorrect format")
        else:
            cred = cls._auth(scopes, credentials_file)
            try:
                cls._save_cred(cred, auth_token_file)
            except OSError as err:
                logging.debug("Could not save auth tokens - {0}".format(err))
            return AuthorizedSession(cred)

    @classmethod
    def get_cred_json(cls, cred):
        # type: (Credentials) -> str
        cred_dict = {
            'token': cred.token,
            'refresh_token': cred.refresh_token,
            'id_token': cred.id_token,
            'scopes': cred.scopes,
            'token_uri': cred.token_uri,
            'client_id': cred.client_id,
            'client_secret': cred.client_secret
        }
        return json.dumps(cred_dict)

    @classmethod
    def _save_cred(cls, cred, auth_file):
        # type: (Credentials, str) -> None

        with open(auth_file, 'w') as f:
            f.write(cls.get_cred_json(cred))


class ApiCaller(object):
    def __init__(self, session):
        # type: (AuthorizedSession) -> None
        self.session = session
        self.pageSize = 50
        self.album_max_size = 20000
        self.debug = False

    def createAlbum(self, album_name):
        create_album_body = json.dumps({"album": {"title": album_name}})
        if self.debug:
            print("createAlbum post", album_name)
        response = self.session.post('https://photoslibrary.googleapis.com/v1/albums', create_album_body)
        if response.status_code != 200:
            print(response.json())
            raise StorageError("unable to download media, response code was not 200: " + str(response.status_code))
        return Album(response.json())

    def listAlbums(self, exclude_non_app_created=False):
        # type: (bool) -> Generator[Album, None, None]
        # print(self.session.headers)
        params = {
            'excludeNonAppCreatedData': exclude_non_app_created,
            'pageSize': self.pageSize
        }
        i = 0
        while True:
            if self.debug:
                print("listAlbums get iteration", i)
            i += 1
            response = self.session.get('https://photoslibrary.googleapis.com/v1/albums', params=params).json()
            if 'albums' in response:
                for album in response['albums']:
                    yield Album(album)
            if 'nextPageToken' in response:
                params['pageToken'] = response['nextPageToken']
            else:
                break

    def listMediaItemsInAlbum(self, album):
        # type: (Album) -> Generator[MediaItem, None, None]
        params = {
            'pageSize': self.pageSize * 2,
            'albumId': album.id
        }
        i = 0
        while True:
            if self.debug:
                print("listMediaItemsInAlbum post iteration", i)
            # traceback.print_stack()
            i += 1
            # response = self.session.get('https://photoslibrary.googleapis.com/v1/mediaItems', params=params).json()
            response = self.session.post('https://photoslibrary.googleapis.com/v1/mediaItems:search',
                                         json.dumps(params)).json()
            # print(response)
            # print('-' * 20)
            if 'mediaItems' in response:
                for media_item in response['mediaItems']:
                    # print(media_item)
                    yield MediaItem(media_item)
            if 'nextPageToken' in response:
                params['pageToken'] = response['nextPageToken']
            else:
                break

    def getSizeOfMediaItem(self, mediaitem):
        # type: (MediaItem) -> int
        response = self.session.head(mediaitem.baseUrl
                                    + '=d',
                                    # + '-w' + str(media_item.mediaMetadata.width)
                                    # + '-h' + str(media_item.mediaMetadata.height)
                                    # + '-c',
                                    allow_redirect=True
                                    )
        if response.status_code != 200:
            print(response.json())
            raise DownloadError("unable to download media, response code was not 200: " + str(response.status_code))
        return int(response.headers['Content-Length'])

    def uploadBytes(self, file_name, file_bytes):
        # type: (str, bytes) -> str
        """
        Uploads bytes to Google servers, returning an umpload token.
        This token can be used to create a MediaItem
        """
        self.session.headers["X-Goog-Upload-File-Name"] = file_name
        self.session.headers["X-Goog-Upload-Protocol"] = 'raw'
        try:
            if self.debug:
                print("uploadBytes post, bytes:", len(file_bytes))
            response = self.session.post('https://photoslibrary.googleapis.com/v1/uploads', file_bytes)
            if response.status_code != 200:
                raise UploadError("unable to upload media, response code was not 200: " + str(response.status_code))
        finally:
            self.session.headers.pop("X-Goog-Upload-File-Name")
            self.session.headers.pop("X-Goog-Upload-Protocol")
        return response.content.decode()

    def createMediaItem(self, upload_tokens_descriptions, target_album):
        # type: (List[Tuple[str, str]], Album) -> List[NewMediaItemResult]
        """
        The returning NewMediaItemResult.mediaItem might not be fully populated, however the id is most certainly
        present. A second call to getMediaItemByID() is necessary.
        """
        if len(upload_tokens_descriptions) == 0:
            raise ValueError('empty token list')
        if len(upload_tokens_descriptions) + target_album.mediaItemsCount > self.album_max_size:
            raise ValueError('current album size and token count exceed max album size')
        new_media_items_list = []
        for upload_token, description in upload_tokens_descriptions:
            new_media_items_list.append({"description": description,
                                         "simpleMediaItem": {"uploadToken": upload_token}
                                         })
        create_body = {"albumId": target_album.id,
                       "newMediaItems": new_media_items_list}
        if self.debug:
            print("createMediaItem post")
        response = self.session.post('https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate',
                                     json.dumps(create_body))
        response_json = response.json()
        if 'newMediaItemResults' not in response_json:
            print(response.status_code, response_json)
        new_media_item_results_list = []
        if 'newMediaItemResults' in response_json:
            for new_media_item_result in response_json['newMediaItemResults']:
                # print(new_media_item_result)
                new_media_item_results_list.append(NewMediaItemResult(new_media_item_result))
        if len(new_media_item_results_list) != len(upload_tokens_descriptions):
            raise StorageError("unable to create all media items. Token count: "
                               + str(len(upload_tokens_descriptions))
                               + ", Media Item count: "
                               + str(len(new_media_item_results_list))
                               + " response code: " + str(response.status_code)
                               + " raw: " + str(response.text))
        return new_media_item_results_list

    def getMediaItemByID(self, mediaitem_id):
        # type: (str) -> MediaItem
        if not mediaitem_id:
            raise ValueError('mediaitem_id is empty')
        if self.debug:
            print("getMediaItemByID get")
        response = self.session.get('https://photoslibrary.googleapis.com/v1/mediaItems/' + mediaitem_id)
        if response.status_code != 200:
            # print(response.json())
            raise NotFoundError("unable to download media, response code was not 200: " + str(response.status_code)+"; response: "
                               +str(response.raw))
        return MediaItem(response.json())

    def getAlbumByID(self, album_id):
        # type: (str) -> Album
        if not album_id:
            raise ValueError('album_id is empty')
        if self.debug:
            print("getAlbumByID get")
        response = self.session.get('https://photoslibrary.googleapis.com/v1/albums/' + album_id)
        if response.status_code != 200:
            print(response.json())
            raise NotFoundError("unable to download album_id, response code was not 200: " + str(response.status_code))
        return Album(response.json())

    def downloadMediaItem(self, media_item):
        # type: (MediaItem) -> bytes
        # print('downloading URL', media_item.baseUrl)
        if self.debug:
            print("downloadMediaItem get")
        response = self.session.get(media_item.baseUrl
                                    + '=d'
                                    # + '-w' + str(media_item.mediaMetadata.width)
                                    # + '-h' + str(media_item.mediaMetadata.height)
                                    # + '-c'
                                    )
        if response.status_code != 200:
            print(response.json())
            raise DownloadError("unable to download media, response code was not 200: " + str(response.status_code))
        return response.content

    def removeMediaItemsFromAlbum(self, album, media_items):
        # type: (Album, List[MediaItem]) -> None
        if len(media_items) != len(set((i.id for i in media_items))):
            raise ValueError("duplicate media items")
        for chunk in chunkiterable_gen(media_items, 50, skip_none=True):
            request_body = {'mediaItemIds': [i.id for i in chunk]}
            if self.debug:
                print("removeMediaItemsFromAlbum post")
            response = self.session.post('https://photoslibrary.googleapis.com/v1/albums/'
                                         + album.id
                                         + ':batchRemoveMediaItems',
                                         json.dumps(request_body))
            if response.status_code != 200:
                print(response.json())
                raise StorageError("unable to remove media, response code was not 200: " + str(response.status_code))

    def addMediaItemsToAlbum(self, album, media_items):
        # type: (Album, List[MediaItem]) -> None
        if len(media_items) != len(set((i.id for i in media_items))):
            raise ValueError("duplicate media items")
        for chunk in chunkiterable_gen(media_items, 50, skip_none=True):
            request_body = {'mediaItemIds': [i.id for i in chunk]}
            if self.debug:
                print("addMediaItemsToAlbum post")
            response = self.session.post('https://photoslibrary.googleapis.com/v1/albums/'
                                         + album.id
                                         + ':batchAddMediaItems',
                                         json.dumps(request_body))
            if response.status_code != 200:
                print(response.json())
                raise StorageError("unable to add media, response code was not 200: " + str(response.status_code))
