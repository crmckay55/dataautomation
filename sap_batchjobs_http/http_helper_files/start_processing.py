# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Environmental variables for the "dataautomation" app service plan in Azure.
# Primary storage (PS) is higher end for managing active data files.
# Secondary storage (SS) is for logs, queues, and archives.
# Changes:
# ---------------------------------------------------------------

import importlib
import logging
import datetime
import pytz

# import our own modules.
try:    # if on azure
    from .manager_blobs import BlobHandler
    from .manager_tables import TableHandler
    from .. import azure_config
except ModuleNotFoundError: # if local
    from sap_batchjobs_http.http_helper_files.manager_blobs import BlobHandler
    from sap_batchjobs_http.http_helper_files.manager_tables import TableHandler
    from sap_batchjobs_http import azure_config


def process_data(filename: str, path: str):
    """First entry point after http call
    This function calls everything needed to load, parse, and save
    the in-process csv blob

    :param filename: passed in by http trigger
    :param path: passed in by http trigger
    :return: message with in process filename
    """

    # load config, parse filename to parameters, and set blob name
    config = azure_config.DefaultConfig()
    file_parameters = _parse_filename(filename)
    destination_blob_name = config.ip_SAPPath + _create_in_process_filename(file_parameters) + '.csv'

    # create blob and table objects
    source_blob = BlobHandler(config.PS_CONNECTION,
                              config.PS_RAW,
                              filename,
                              path)

    destination_blob = BlobHandler(config.PS_CONNECTION,
                                   config.PS_INPROCESS,
                                   destination_blob_name
                                   )

    # TODO: Write to log table with results.
    # log_table = TableHandler(config.SS_CONNECTION,
    #                         config.SS_LOGSTABLE)

    source_contents = source_blob.get_blob(filename)

    # dynamically load correct parser module based on transaction in parsed filename
    try:
        tx = file_parameters['transaction']
        transaction_processor = importlib.import_module(f'sap_batchjobs_http.http_helper_files.tx_{tx}', package=None)
        logging.info(f'Found module for {tx} without __app__')
        
    except:
        transaction_processor = importlib.import_module(f'__app__.http_helper_files.tx_{tx}', package=None)
        logging.info(f'Found module for {tx} with __app__')

    parsed_df = transaction_processor.parse_batch_file(source_contents, file_parameters['function'])

    # Add filename column for later use by ADF in sink filename "as data in column"
    parsed_df['filename'] = file_parameters['event'] + '/' + \
                            file_parameters['transaction'] + '/' + \
                            file_parameters['date'] + '_' + \
                            file_parameters['step'] + '.csv'

    # write in process blob and delete raw blob
    destination_blob.write_blob(parsed_df, 'csv')
    source_blob.delete_bob()

    # TODO: write status to log table
    # TODO: need to catch exceptions, and still write to log table.

    return f'success with {destination_blob_name}'


def _parse_filename(filename: str):
    """Separate the filename into useable components
    :param filename: filename passed by data factory
    :return: parameters made out of the filename
    """

    #          Job SAP-Carseland 2021-IW38_01, Step 1.htm <- update this!
    # step 1:  strip                                      <- get rid of prefix
    # step 2:  xxxxxxxx      split[0]        ,   split[1] <- get rid of suffix
    # step 3:  xxxxxxxx   split[0]   -split[1]xxxxxxxxxxxx<- separate elements
    # step 4:  xxxxxxxx           split[0]_split[1]       <- isolate transaction
    #          Job SAP-Carseland 2021-IW38_01-20200606, Step 1.htm
    #          xxxxxxx-  split [0]   -split[1]-split[2]xxx<- if it comes with a date
    #
    # TODO: create invalid filename routine to check for proper formatting

    preparse1 = filename.strip('Job SAP-')      # Step 1: remove prefix
    preparse2 = preparse1.split(',')[0]         # Step 2: remove suffix

    # get the "Step x" to allow for multiple files per date and transaction
    # get right have of column split [1], and left halve at dot split [0], strip spaces from left.
    step = preparse1.split(',')[1].split('.')[0].lstrip()

    event = preparse2.split('-')[0]             # get event name, left-most split of -
    transaction = preparse2.split('-')[1].split('_')[0] # get transaction name, second index, left part of _ split
    version = preparse2.split('-')[1].split('_')[1]     # get transaction version, second index, right part of _ split

    try:    # if there is index 2 (3 parameters) in preparse, then it's a manual upload with a snapshot date
        snap_date = preparse2.split('-')[2]
    except:  # otherwise use the current date
        dt_now = pytz.utc.localize(datetime.datetime.utcnow())
        snap_date = dt_now.astimezone(pytz.timezone("America/Edmonton")).strftime("%Y%m%d")

    parameters = {'event': event,
                  'transaction': transaction,
                  'version': version,
                  'date': snap_date,
                  'step': step,
                  'function': transaction + '_' + version}

    return parameters


def _create_in_process_filename(parsed_filename: dict):
    """Rearrange the components of the source filename

    :param parsed_filename:
    :return: in_process_filename
    """

    # TODO: figure out sink path name
    in_process_filename = parsed_filename['transaction'] + '/' + \
                          parsed_filename['event'] + '-' \
                          + parsed_filename['transaction'] + '_' + parsed_filename['version'] \
                          + '-' + parsed_filename['date'] + '-' + \
                          parsed_filename['step']

    return in_process_filename

