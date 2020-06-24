# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 5, 2020
# Http trigger will call this function first, check for the file
# name and call the start_processing function. The file name
# has all of the required information to route the data in this
# function to be processed the correct way.
# Changes:
# ---------------------------------------------------------------

import logging
import json
import azure.functions as func

try:     # if azure
    from . import azure_config
    from .http_helper_files import start_processing
except ModuleNotFoundError:  # if local
    from sap_batchjobs_http import azure_config
    from sap_batchjobs_http.http_helper_files import start_processing


def main(req: func.HttpRequest) -> func.HttpResponse:

    # get parameters from HTTP trigger
    source_container = req.params.get('source_container')
    sink_container = req.params.get('sink_container')
    path = req.params.get('source_path')
    datatype = req.params.get('datatype')
    factory_info = req.params.get('factoryinfo')
    pipeline_run_id = req.params.get('pipelinerunid')
    pipeline_start_time = req.params.get('starttime')

    if not path:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            source_container = req_body.get('source_container')
            sink_container = req_body.get('sink_container')
            path = req_body.get('source_path')
            datatype = req_body.get('datatype')
            factory_info = req_body.get('factoryinfo')
            pipeline_run_id = req_body.get('pipelinerunid')
            pipeline_start_time = req_body.get('starttime')


    if path:
        logging.info(f'Python HTTP trigger function processed a request with {path} and {datatype}.')

        # try and process file, and return the resuting message
        response = {'results': start_processing.process_data(source_container, path,
                                                             sink_container, datatype,
                                                             factory_info, pipeline_run_id, pipeline_start_time)}
        
        func.HttpResponse.mimetype = 'application/json'
        func.HttpResponse.charset = 'utf-8'

        logging.info(f'Json response will be: {response}')
      
        return func.HttpResponse(body=json.dumps(response), status_code=200)

    # TODO: enable returning with error

    else:
        response = {"inprogress_blob_name": "error somewhere"}
        return func.HttpResponse(body=response,
                                 status_code=200)
