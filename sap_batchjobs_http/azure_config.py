# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Environmental variables for the "dataautomation" app service plan in Azure.
# Primary storage (PS) is higher end for managing active data files.
# Secondary storage (SS) is for logs, queues, and archives.
# Changes:
# ---------------------------------------------------------------

import os


class DefaultConfig:

    PS_CONNECTION = os.getenv('PRIMARYSTORAGE_CONNECTIONSTRING')
    PS_RAW = os.getenv('PRIMARYSTORAGE_RAW')
    PS_INPROCESS = os.getenv('PRIMARYSTORAGE_INPROCESS')
    PS_FINAL = os.getenv('PRIMARYSTORAGE_FINAL')

    SS_CONNECTION = os.getenv('SECONDARYSTORAGE_CONNECTIONSTRING')
    SS_ARCHIVEBLOB = os.getenv('SECONDARYSTORAGE_ARCHIVEBLOB')
    SS_LOGSTABLE = os.getenv('SECONDARYSTORAGE_LOGSTABLE')
    SS_QUEUE = os.getenv('SECONDARYSTORAGE_QUEUE')

    ip_SAPPath = 'SAPBatchReports/'



