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
import datetime, pytz

try:
    from .manager_blobs import BlobHandler
    from .manager_tables import TableHandler
    from .. import azure_config
except ModuleNotFoundError:
    from sap_batchjobs_http.http_helper_files.manager_blobs import BlobHandler
    from sap_batchjobs_http.http_helper_files.manager_tables import TableHandler
    from sap_batchjobs_http import azure_config


def process_data(filename: str, path: str):

    config = azure_config.DefaultConfig()
    file_parameters = _parse_filename(filename)
    destination_blob_name = _create_in_process_filename(file_parameters) + '.csv'

    source_blob = BlobHandler(config.PS_CONNECTION,
                              config.PS_RAW,
                              filename,
                              path)

    destination_blob = BlobHandler(config.PS_CONNECTION,
                                   config.PS_INPROCESS,
                                   destination_blob_name,
                                   '')

    # TODO: Write to log table with results.
    log_table = TableHandler(config.SS_CONNECTION,
                             config.SS_LOGSTABLE)

    source_contents = source_blob.get_blob(filename)

    try:
        tx = file_parameters['transaction']
        transaction_processor = importlib.import_module(f'sap_batchjobs_http.http_helper_files.tx_{tx}', package=None)
        logging.info(f'Found module for {tx} without __app__')
        
    except:
        transaction_processor = importlib.import_module(f'__app__.http_helper_files.tx_{tx}', package=None)
        logging.info(f'Found module for {tx} with __app__')

    parsed_df = transaction_processor.parse_batch_file(source_contents, file_parameters['function'])

    destination_blob.write_blob(parsed_df, 'csv')
    source_blob.delete_bob()

    # TODO: write status to log table
    # TODO: need to catch exceptions, and still write to log table.

    return file_parameters['event'], file_parameters['transaction'], file_parameters['function'], file_parameters['date']


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

    step1 = filename.strip('Job SAP-')  # Step 1: remove prefix
    step2 = step1.split(',')[0]         # Step 2: remove suffix
    event = step2.split('-')[0]         # get event name
    transaction = step2.split('-')[1].split('_')[0]
    version = step2.split('-')[1].split('_')[1]

    try:    # if there is index 2, then the manual file has the date
        snap_date = step2.split('-')[2]
    except: # otherwise use the current date
        dt_now = pytz.utc.localize(datetime.datetime.utcnow())
        snap_date = dt_now.astimezone(pytz.timezone("America/Edmonton")).strftime("%Y%m%d")

    parameters = {'event': event,
                  'transaction': transaction,
                  'version': version,
                  'date': snap_date,
                  'function': transaction + '_' + version}

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

