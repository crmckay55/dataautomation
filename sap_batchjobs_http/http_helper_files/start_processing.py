# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Environmental variables for the "dataautomation" app service plan in Azure.
# Primary storage (PS) is higher end for managing active data files.
# Secondary storage (SS) is for logs, queues, and archives.
# Changes:
# ---------------------------------------------------------------

import os, importlib, logging, tempfile

try:
    from . import manager_blobs
    from . import manager_tables
    from .. import azure_config
except ModuleNotFoundError:
    from sap_batchjobs_http.http_helper_files import manager_blobs
    from sap_batchjobs_http.http_helper_files import manager_tables
    from sap_batchjobs_http import azure_config


def process_data(filename: str):

    config = azure_config.DefaultConfig()
    file_parameters = _parse_filename(filename)
    in_process_blob = _create_in_process_filename(file_parameters) + '.csv'

    source_blob = manager_blobs.BlobHandler(config.PS_CONNECTION,
                                            config.PS_RAW,
                                            filename)

    destination_blob = manager_blobs.BlobHandler(config.PS_CONNECTION,
                                                 config.PS_INPROCESS,
                                                 in_process_blob)

    log_table = manager_tables.TableHandler(config.SS_CONNECTION,
                                            config.SS_LOGSTABLE)


    source_contents = source_blob.get_blob(filename)

    # TODO: figure out correct transaction to call.
    # TODO: import said module dynamically, allowing for local vs. azure imports
    # TODO: call transaction module dynamically with contents and version (e.g. IW38_01)
    # TODO: create in process filename

    # TODO: write data to destination blob.
    # TODO: add a paramater to specify how to write: json, CSV, delimiters etc.
    destination_blob.write_blob('')  # TODO: add dynamic filename.

    # TODO: write status to log table
    # TODO: need to catch exceptions, and still write to log table.

    # TODO return the temporary filename so it can be returned to the calling data factory for further processing.
    return in_process_blob


def _parse_filename(filename: str):
    """Separate the filename into useable components

    :param filename:
    :return: parameters
    """

    #          Job SAP-Carseland 2021-IW38_01-20200523, Step 1.htm <- update this!
    # step 1:  strip                                               <- get rid of prefix
    # step 2:  xxxxxxxx               split[0]        ,   split[1] <- get rid of suffix
    # step 3:  xxxxxxxx   split[0]   -split[1]-  split[2]          <- separate elements
    # step 4:  xxxxxxxx           split[0]_split[1]                <- isolate transaction
    #

    name = filename.strip('Job SAP-')  # Step 1: remove prefix
    name = name.split(',')[0]          # Step 2: remove suffix
    tx = name.split('_')               # get transaction only

    parameters = {'event': name[0],
                  'transaction': tx[0],
                  'version': tx[1],
                  'date': name[2]}

    return parameters


def _create_in_process_filename(parsed_filename: dict):
    """Rearrange the components of the source filename

    :param parsed_filename:
    :return: in_process_filename
    """

    in_process_filename = parsed_filename['event'] + '-' \
                         + parsed_filename['transaction'] + '_' + parsed_filename['version'] \
                         + '-' + parsed_filename['date']

    return in_process_filename

