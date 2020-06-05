# ---------------------------------------------------------------
# author: Chris McKay
# version: 1.0
# date: June 4, 2020
# Processes all IW38 transactions into a dataframe.
# Changes:
# ---------------------------------------------------------------

import sys
import logging
import json

from bs4 import BeautifulSoup
import pandas as pd


def parse_batch_file(contents, version: str):

    # TODO: dynamically call method based on version
    func = getattr(sys.modules[__name__], "_" + version.lower())
    df = func(contents)

    return df


def _iw38_01(contents) -> pd.DataFrame:
    """ Process IW38 version 1 layout into pandas dataframe
    :param contents: source blob contents .htm file
    :return: dataframe of parsed .htm contents
    """

    # parse contents with soup
    soup = BeautifulSoup(contents, "html.parser")

    # strip out list elements
    stripped_lists = soup('table', {"class": "list"})

    df = pd.DataFrame

    # iterate through lists to get bodies
    for idx_lst, lst in enumerate(stripped_lists):
        stripped_bodies = lst('tbody')

        # iterate through bodies to get rows
        for idx_body, body in enumerate(stripped_bodies):

            # if first list and body, get header rows
            if idx_lst == 0 and idx_body == 0:
                hdr_row = body('tr')
                columns = hdr_row[0]('td')
                keys = _iw38_01_get_headers(columns)

            # if not first list, skip body 0 which is the header again
            elif idx_body > 0:
                stripped_rows = body('tr')

                # iterate through rows and get columns
                for idx_row, row in enumerate(stripped_rows):
                    columns = row('td')

                    df2 = _iw38_01_get_row(columns, keys)
                    df = df.append(df2, ignore_index=True)

    return df


def _iw38_01_get_row(column_data, headers) -> pd.DataFrame:
    """

    :param column_data:
    :param headers:
    :return:
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


def _iw38_01_get_headers(columns) -> list:
    """

    :param columns:
    :return:
    """
    header_list = []

    for column_idx, column in enumerate(columns):
        value = column('nobr')
        header_list.append(value[0].text.strip().replace('\xa0', ' '))

    return header_list



