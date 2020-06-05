# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Manages all blob read, writes, and deletes.
# Changes:
# ---------------------------------------------------------------
import logging
import tempfile
from azure.storage.blob import BlobServiceClient


class BlobHandler:

    def __init__(self, connection_string, container, path):
        self.connection_string = connection_string
        self.container = container
        self.folder = folder

    def get_blob(self, filename):
        contents = []
        # open blob and write to local file
        # read open file to object
        # return contents
        return contents

    def write_blob(self, filename):
        # write blob to container and folder defined in the class
        # return status
        pass

    def delete_bob(self, filename):
        # delete blob in container and folder defined in the class
        # return status
        pass
