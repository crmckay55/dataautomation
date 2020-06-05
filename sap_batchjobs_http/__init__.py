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
import azure.functions as func

try:     # if azure
    from . import azure_config
    from .http_helper_files import start_processing
except ModuleNotFoundError:  # if local
    from sap_batchjobs_http import azure_config
    from sap_batchjobs_http import start_processing


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # TODO: standardize the body passed.  This will be done in data factory
    name = req.params.get('filename')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('filename')

    if name:
        return func.HttpResponse(f'{name} does exist',
                                 status_code=200)

        #func.HttpResponse(start_processing.process_data(name))
    else:
        return func.HttpResponse(f'Error-{name} does not exist',
                                 status_code=200)
