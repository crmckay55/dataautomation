# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Manages all blob read, writes, and deletes.
# Changes:
# ---------------------------------------------------------------
import os
import logging
import tempfile
import pandas as pd
from azure.storage.blob import BlobClient, BlobServiceClient
import azure.core.exceptions as ae


class BlobHandler:

    def __init__(self, connection_string: str, container: str, path: str = ''):
        """Sets up connection to container,and specific path in container
           Each BlobHandler object is for one container/path combination

        :param connection_string: azure connection string
        :param container: container to connect to
        :param path: path within container, can be null and set dynamically, no filename in path
        """
        self._connection_string = connection_string
        self._container = container
        self.path = path    # can be set dynamically after object creation

    def read_blob_csv_to_df(self, filename: str):
        """ Connects to blob and returns contents

        :param filename: full file name without path or container
        :return: contents of file
        """

        # TODO wrap in try, return error, or contents
        blob_client = self._create_client(filename)

        # get local tempfile path and create a tempfile name
        temp_file_path = tempfile.gettempdir()
        temp_file = os.path.join(temp_file_path, "tempfile")

        # download blob to temp file
        with open(temp_file, 'wb') as f:
            f.write(blob_client.download_blob().readall())

        df = pd.read_csv(temp_file,  delimiter='\t', encoding='UTF-16', engine='python')

        logging.info(f'Read file {filename} of size {len(df)}')

        blob_client = None

        return df

    def write_csv_to_blob(self, df, filename: str):
        """writes a tab-delimited file to container and path

        :param df: pandas dataframe to write to file
        :param filename:
        :return:
        """
        # TODO warp in try, return status

        try:
            blob_client = self._create_client(filename)
            output = df.to_csv(index=False, sep='\t')
            blob_client.upload_blob(output)
            self.delete_blob_file(filename)

        except Exception as e:
            self.delete_blob_file(filename)
            output = df.to_csv(index=False, sep='\t')
            blob_client.upload_blob(output)

        blob_client = None

    def delete_blob_file(self, filename: str):
        """Deletes a blob

        :param filename:
        :return: None
        """

        # TODO wrap in try, return status
        blob_client = self._create_client(filename)
        blob_client.delete_blob(delete_snapshots=None)
        blob_client = None

    def get_blob_list(self):
        """Returns blobs in object's container

        :return: generator with list of blobs
        """

        service = self._create_service()
        client = service.get_container_client(self._container)

        generator = client.list_blobs(name_starts_with=self.path)

        service = None
        client = None

        return generator

    def _create_service(self) -> BlobServiceClient:
        return BlobServiceClient.from_connection_string(conn_str=self._connection_string)

    def _create_client(self, filename: str) -> BlobClient:
        """Creates blob client object

        :param filename: name of file to open
        :return: blob client object
        """

        # ensure last character on path is '/' to allow for filename appending
        if self.path[-1] != '/':
            self.path = self.path + '/'

        file_to_open = self.path + filename
        print(file_to_open)
        return BlobClient.from_connection_string(conn_str=self._connection_string,
                                                 container_name=self._container,
                                                 blob_name=file_to_open)
