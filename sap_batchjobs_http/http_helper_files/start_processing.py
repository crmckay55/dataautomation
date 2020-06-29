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
    from ..http_helper_files import sap_batch
except ModuleNotFoundError:  # if local
    from sap_batchjobs_http.http_helper_files.manager_blobs import BlobHandler
    from sap_batchjobs_http.http_helper_files import sap_batch


def process_data(source_container: str, source_path: str,
                 sink_container: str, datatype: str,
                 factory_info: str, pipeline_run_id: str, pipeline_start_time: str):
    """ First entry point after http call
    This function calls everything needed to load, parse, and save
    the in-process csv blob

    :param source_container:
    :param source_path:
    :param sink_container:
    :param datatype:
    :param factory_info:
    :param pipeline_run_id:
    :param pipeline_start_time:
    :return:
    """

    if datatype == 'sap_batch':
        try:
            from ..http_helper_files import sap_batch
        except ModuleNotFoundError:
            from sap_batchjobs_http.http_helper_files import sap_batch

        sap_batch.start(source_container, source_path, sink_container)

        return 'SAP_Batch Done'

    elif datatype == 'obiee_agent':
        try:
            from ..http_helper_files import obiee_agent
        except ModuleNotFoundError:
            from sap_batchjobs_http.http_helper_files import obiee_agent

        obiee_agent.start(source_container, source_path, sink_container)

        return 'OBIEE_Agent Done'

        pass

    # TODO: Dynamically determine while handler to run based on data type

    return 'No Processor Found'