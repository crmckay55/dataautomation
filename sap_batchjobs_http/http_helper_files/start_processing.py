# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Environmental variables for the "dataautomation" app service plan in Azure.
# Primary storage (PS) is higher end for managing active data files.
# Secondary storage (SS) is for logs, queues, and archives.
# Changes:
# ---------------------------------------------------------------

# TODO update for new file format!!

import importlib
import logging
import datetime
import pytz

# import our own modules.
try:    # if on azure
    from .manager_blobs import BlobHandler
    from .manager_tables import TableHandler
    from . import sap_batch
except ModuleNotFoundError: # if local
    from sap_batchjobs_http.http_helper_files.manager_blobs import BlobHandler
    from sap_batchjobs_http.http_helper_files.manager_tables import TableHandler
    from sap_batchjobs_http.http_helper_files import sap_batch


def process_data(source_container: str, path: str,
                 sink_container: str, datatype: str,
                 factory_info: str, pipeline_run_id: str, pipeline_start_time: str):
    """ First entry point after http call
    This function calls everything needed to load, parse, and save
    the in-process csv blob

    :param source_container:
    :param path:
    :param sink_container:
    :param datatype:
    :param factory_info:
    :param pipeline_run_id:
    :param pipeline_start_time:
    :return:
    """

    sap_batch.start(source_container, path, sink_container)

    # TODO: Dynamically determine while handler to run based on data type

    return 'Done'