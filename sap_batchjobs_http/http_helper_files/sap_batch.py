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
try:  # if on azure
    from .manager_blobs import BlobHandler
    from .. import azure_config
except ModuleNotFoundError:  # if local
    from sap_batchjobs_http.http_helper_files.manager_blobs import BlobHandler
    from sap_batchjobs_http import azure_config


def start(source_container: str, source_path: str, sink_container: str):
    config = azure_config.DefaultConfig()

    # Create source blob object
    source_blob = BlobHandler(config.PS_CONNECTION,
                              source_container,
                              source_path)

    # Create sink blob object
    destination_blob = BlobHandler(config.PS_CONNECTION,
                                   sink_container)  # path will change so don't pass a path

    # get files from source container and path
    files_to_process = source_blob.get_blob_list()

    # for each file in files_to_process
    for file in files_to_process:
        source_file, adf_path, in_progress_path, in_progress_file = _get_new_path_file(file, source_path)

        # read into dataframe
        df = source_blob.read_blob_csv_to_df(source_file)

        # add filename column for ADF to use
        df['filename'] = adf_path
        df.columns = df.columns.str.strip()

        # change column names to generic so that there is consistency for azure data wrangling
        # SAP will change column names from time to time, but we've mapped columns based on indes
        # in data wrangling flow
        col_count = len(df.columns)
        print(col_count)
        col_names = list(range(1, col_count+1))
        print(col_names)
        df.columns = col_names


        # write to destination blob and cleanup
        destination_blob.path = in_progress_path
        destination_blob.write_csv_to_blob(df, in_progress_file)
        source_blob.delete_blob_file(source_file)


def _get_new_path_file(file: str, path: str):
    """parses the blob filename and path for use with manager_blobs class

    :param file: full path and file returned by blob list generator
    :param path: path passed by ADF to use for source
    :return: source_file, adf_path, in_progress_sink_path, in_progress_sink_file
    """

    # template file: SAPBatchReports/PBI-Redwater TAR 2020-IW38_20200624.csv
    # template path: SAPBatchReports/
    extracted_filename = file['name']  # starting point full path in raw container

    # parse to construct everything`
    file_without_path = extracted_filename.split(path)[1]  # get new filename
    file_no_flag = file_without_path.split('PBI-')[1]  # remove PBI flag
    event_tx_only = file_no_flag.split('_')[0]  # get event & tx for later processing
    event_only = event_tx_only.split('-')[0]  # left hand side with event name only
    transaction_only = event_tx_only.split('-')[1]  # right hand side with transaction only
    date_only = file_no_flag.split('_')[1].split('.')[0]  # date only

    # create all return variables
    adf_path = path + event_only + '/' + transaction_only + '/' + date_only + '.csv'
    in_progress_sink_path = path + transaction_only + '/'
    in_progress_sink_file = file_without_path
    source_file = file_without_path

    print(adf_path)
    return source_file, adf_path, in_progress_sink_path, in_progress_sink_file
