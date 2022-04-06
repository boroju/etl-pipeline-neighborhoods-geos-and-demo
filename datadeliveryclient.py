import json
import requests
from urllib.request import urlopen
from datetime import datetime
import utilities as utils


class DataDeliveryClientError(Exception):
    pass


class DataDeliveryClient:

    def __init__(self, api_host, api_key, file_ext):
        self._api_host = api_host
        self._api_key = api_key
        self._file_ext = file_ext
        self._asset_manifest = self.api_host + '/' + self._api_key + '/' + self._file_ext

    # function to get value of _api_host
    def get_api_host(self):
        return self._api_host

    # function to delete _api_host attribute
    def del_api_host(self):
        del self._api_host

    api_host = property(get_api_host)

    # function to get value of _api_host
    def get_api_key(self):
        return self._api_key

    # function to delete _api_host attribute
    def del_api_key(self):
        del self._api_key

    api_key = property(get_api_key)

    # function to get value of _file_ext
    def get_file_ext(self):
        return self._file_ext

    # function to delete _file_ext attribute
    def del_file_ext(self):
        del self._file_ext

    file_ext = property(get_file_ext)

    # function to get value of _asset_manifest
    def get_asset_manifest(self):
        return self._asset_manifest

    # function to delete _asset_manifest attribute
    def del_asset_manifest(self):
        del self._asset_manifest

    asset_manifest = property(get_asset_manifest)

    def get_last_updated_from_json(self):

        url = self.asset_manifest

        try:
            response = urlopen(url)
            data_json = json.loads(response.read())
            upload_date = datetime.strptime(data_json['result']['last-updated'], '%Y-%m-%dT%H:%M:%S.%fZ')
            upload_date = upload_date.strftime('%Y%m%d%H%M%S')
            upload_date = upload_date[0:8]
            print('Returning last upload date from asset manifest...')
            return upload_date
        except Exception as exception:
            return exception

    def get_download_file_urls(self, folder_date):

        url = self.asset_manifest

        try:
            response = urlopen(url)
            data_json = json.loads(response.read())
            files_urls = []
            for i in data_json['result']['inventory']:
                if int(folder_date) < int(i['filename'].partition('-')[0]):
                    files_urls.append(i['url'])
            print('Returning a list of urls for downloading files...')
            return files_urls
        except Exception as exception:
            return exception

    @staticmethod
    def get_zip_filenames_from_urls(files_urls):

        filenames = []

        try:
            for url in files_urls:
                req = requests.get(url, allow_redirects=True)
                filenames.append(utils.get_filename_from_url(req.headers.get('content-disposition')))
            print('Returning filenames list for downloading files...')
            return filenames
        except Exception as exception:
            return exception
