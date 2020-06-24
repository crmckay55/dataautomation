# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 24, 2020
# ingests all sap batch files (tab delim csv's), adds a column
# that contains the file path/filename for Azure Data Factory
# to use when transforming the data.
# Changes:
# ---------------------------------------------------------------


# import our own modules.
try:    # if on azure
    from .manager_blobs import BlobHandler
    from .. import azure_config
except ModuleNotFoundError:  # if local
    from sap_batchjobs_http.http_helper_files.manager_blobs import BlobHandler
    from sap_batchjobs_http import azure_config


def start(source_container: str, path: str, sink_container: str):

    config = azure_config.DefaultConfig()

    # Create source blob object
    source_blob = BlobHandler(config.PS_CONNECTION,
                              source_container,
                              path)

    # Create sink blob object
    destination_blob = BlobHandler(config.PS_CONNECTION,
                                   sink_container)

    # get files from source container and path
    files_to_process = source_blob.get_blob_list()

    # for each file in files_to_process
    for file in files_to_process:
        filename, sink_column, sink_path, sink_filename = _get_new_path_file(file)

        df = source_blob.read_blob_csv_to_df(filename)

        df['filename'] = sink_column

        destination_blob.path = sink_path
        destination_blob.write_csv_to_blob(df, sink_filename)
        source_blob.delete_blob_file(filename)


def _get_new_path_file(file: str):

    extracted_filename = file['name']
    last_slash = extracted_filename.rfind('/')

    # get bare filename 'yyyymmdd.csv'
    filename = extracted_filename[last_slash+1:]

    parsed_filename = filename.strip('PBI-')
    last_underscore_index = parsed_filename.rfind('_')
    last_dash_index = parsed_filename.rfind('-')

    # get full filename and path for dataframe insertion
    filename_list = list(parsed_filename)
    filename_list[last_underscore_index] = '/'
    filename_list[last_dash_index] = '/'

    # full sync name with folders for dataframe and ADF to use
    full_sink_filename = ''.join(filename_list)

    sink_path_only = full_sink_filename[0:last_underscore_index]
    sink_filename_only = full_sink_filename[last_underscore_index+1:]

    return filename, full_sink_filename, sink_path_only, sink_filename_only


