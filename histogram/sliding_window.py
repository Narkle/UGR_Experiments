"""Test new sliding window technique, should be simpler and
hopefully faster
"""

import argparse
import os

import pandas as pd
import numpy as np
import datetime

# NETFLOW variables
hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]

totRows, processedRows = 0, 0

def readAndProcessCSV(filename, w=15):
    global totRows, processedRows

    df_curr, curr_window = None, None
    for df in pd.read_csv(filename, chunksize=10**3, iterator=True):
        df['te'] = pd.to_datetime(df['te']).dt.floor("%dT" % w)
        totRows += df.shape[0]
        grouped = df.groupby(['te'])
        for window, df in grouped:
            if df_curr is None:
                df_curr = df
                curr_window = window
                continue
            if window == curr_window:
                df_curr = df_curr.append(df)
                print("[+] Appended")
            else:
                process(curr_window, df_curr)
                df_curr, curr_window = df, window
    process(curr_window, df_curr)


def process(window_start, df):
    print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))
    global processedRows
    processedRows += df.shape[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)

    args = parser.parse_args()
    filename = args.f

    readAndProcessCSV(filename)
    print('[+] Total: %d, Processed: %d' % (totRows, processedRows))
    if totRows == processedRows:
        print('[+] PASSED')
    else:
        print('[-] FAILED')
