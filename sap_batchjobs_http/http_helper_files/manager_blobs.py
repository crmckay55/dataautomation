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
from azure.storage.blob import BlobClient


class BlobHandler:

    def __init__(self, connection_string, container, path):
        self.connection_string = connection_string
        self.container = container
        self.path = path
        self.blob_client = BlobClient.from_connection_string(conn_str=self.connection_string,
                                                             container_name=self.container,
                                                             blob_name=self.path)

    def get_blob(self, filename):

        temp_file_path = tempfile.gettempdir()
        temp_file = os.path.join(temp_file_path, "file.htm")

        with open(temp_file, 'wb') as f:
            f.write(self.blob_client.download_blob().readall())

        with open(temp_file, "r") as f:
            contents = f.read()

        logging.info(f'Read file {filename} of size {len(contents)}')

        return contents

    def write_blob(self, contents):

        self.blob_client.upload_blob(contents)
        # return status

    def delete_bob(self, filename):
        # delete blob in container and folder defined in the class
        # return status
        pass
