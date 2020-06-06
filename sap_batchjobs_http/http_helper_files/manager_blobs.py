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
from azure.storage.blob import BlobServiceClient


class BlobHandler:

    def __init__(self, connection_string, container, path):
        self.connection_string = connection_string
        self.container = container
        self.path = path

    def get_blob(self, filename):

        temp_file_path = tempfile.gettempdir()
        temp_file = os.path.join(temp_file_path, "file.htm")

        blob_services_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)
        blob_container_client = blob_services_client.get_container_client(self.container)
        blob_client = blob_container_client.get_blob_client(self.path + filename)

        with open(temp_file, 'wb') as f:
            f.write(blob_client.download_blob().readall())

        with open(temp_file, "r") as f:
            contents = f.read()

        logging.info(f'Read file {filename} of size {len(contents)}')

        return contents

    def write_blob(self, filename):
        # write blob to container and folder defined in the class
        # return status
        pass

    def delete_bob(self, filename):
        # delete blob in container and folder defined in the class
        # return status
        pass
