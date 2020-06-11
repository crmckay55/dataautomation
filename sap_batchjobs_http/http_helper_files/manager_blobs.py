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

    def __init__(self, connection_string, container, filename, path):
        self.connection_string = connection_string
        self.container = container
        self.path = path + '/' + filename
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

    def write_blob(self, df, file_type):

        if file_type == "csv":
            output = df.to_csv(index=False, sep='\t')
            self.blob_client.upload_blob(output)
        else:
            pass
        # TODO: return something

    def delete_bob(self):
        self.blob_client.delete_blob(delete_snapshots=False)
