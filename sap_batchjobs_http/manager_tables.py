# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Manages all table read, writes, and deletes.
# Changes:
# ---------------------------------------------------------------
import uuid
import logging
from azure.cosmosdb.table.tableservice import TableService, TableBatch
from azure.cosmosdb.table.models import EntityProperty, EdmType

class TableHandler:

    def __init__(self, connection_string, table):
        self.connection_string = connection_string
        self.table = table

    def get_data(self, query):  # TODO: need to figure out what to pass here.  Dict?
        contents = []
        # open blob and write to local file
        # read open file to object
        # return contents
        return contents

    def write_data(self, filename):  # TODO: I think this should be a dict
        # write blob to container and folder defined in the class
        # return status
        pass

    def delete_data(self, filename):  # TODO: again, should be dict?
        # delete blob in container and folder defined in the class
        # return status
        pass
