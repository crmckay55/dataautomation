# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Processes all ZI73 transactions into a dataframe.
# Changes:
# ---------------------------------------------------------------

import sys
import logging
import json

from bs4 import BeautifulSoup
import pandas as pd


def parse_batch_file(contents, function: str):
    """

    :param contents: blob data
    :param function: what version of the function to call
    :return: dataframe
    """
    # TODO: dynamically call method based on version
    func = getattr(sys.modules[__name__], '_' + function.lower())
    df = func(contents)

    return df


def _zi73_01(contents) -> pd.DataFrame:
    """ Process IW38 version 1 layout into pandas dataframe
    :param contents: source blob contents .htm file
    :return: dataframe of parsed .htm contents
    """

    zi73_start_table = 2

    # parse contents with soup
    soup = BeautifulSoup(contents, "html.parser")

    # strip out list elements
    stripped_lists = soup('table', {"class": "list"})

    # iterate through lists to get bodies
    for idx_lst, lst in enumerate(stripped_lists):

        if idx_lst < zi73_start_table:
            pass
        else:
            stripped_bodies = lst('tbody')

            # iterate through bodies to get rows
            for idx_body, body in enumerate(stripped_bodies):

                # if first list and body, get header rows
                if idx_lst == zi73_start_table and idx_body == 0:
                    hdr_row = body('tr')
                    columns = hdr_row[0]('td')
                    keys = _zi73_01_get_headers(columns)
                    df = pd.DataFrame(columns=keys)

                # if not first list, skip body 0 which is the header again
                elif idx_body > 0:
                    stripped_rows = body('tr')

                    # iterate through rows and get columns
                    for idx_row, row in enumerate(stripped_rows):
                        columns = row('td')

                        df = df.append(_zi73_01_get_row(columns, keys), ignore_index=True)

    return df


def _zi73_01_get_row(column_data, headers) -> pd.DataFrame:
    """get the data for the current row
    :param column_data: list of entities with data
    :param headers: list of headers
    :return: dataframe for the current row
    """
    df = pd.DataFrame(columns=headers)
    current_row = {}

    for column_idx, column in enumerate(column_data):
        value = column('nobr')
        if column_idx == 0:
            idx = 2
        else:
            idx = 0

        current_row.update({headers[column_idx]: value[idx].text.strip().replace('\xa0', ' ')})

    df = df.append(current_row, ignore_index=True)

    return df


def _zi73_01_get_headers(columns) -> list:
    """parse the headers of the table
    :param columns:
    :return:
    """
    header_list = []

    for column_idx, column in enumerate(columns):
        value = column('nobr')
        header_list.append(value[0].text.strip().replace('\xa0', ' '))

    return header_list



