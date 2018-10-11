"""Compute KL timeseries
Usage: python kl_timeseries netflow.csv
"""

import argparse
import os

import pandas as pd
import numpy as np
import datetime

from voting import HistogramVoter

hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
windowLength = pd.Timedelta("15 minutes")
output_col = None
output = []
timestamps = []

voting = None

window_start = None
window_end = None
df_window = pd.DataFrame(columns=cols)

totRows = 0


def process(window_start, df):
    print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))
    row = voting.hist_kl(df)
    if row:
        output.append([window_start] + row)


def readAndProcessCSV(filename, w=15):
    global totRows, processedRows

    df_curr, curr_window = None, None
    # erronous lines will be skipped
    for df in pd.read_csv(filename, chunksize=10**8, iterator=True, error_bad_lines=False):
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
            else:
                process(curr_window, df_curr)
                df_curr, curr_window = df, window
    process(curr_window, df_curr)


def outfilename(filepath):
    basename = os.path.basename(filepath)
    last_idx = basename.rindex('.')
    name_wo_ext = basename[:last_idx]

    return "%s.kl.csv" % name_wo_ext

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('k', help='number of clones', type=int)
    parser.add_argument('m', help='hash function length (2**m)', type=int)
    parser.add_argument('--D', help='directory to save the output', type=str)

    args = parser.parse_args()
    filename = args.f
    k = args.k
    m = args.m

    voting = HistogramVoter(w=15,m=m,k=k,l=5) # l is any arbitrary number
    output_col = ['time'] + ["%s_%s" % (col, i) for col in hist_cols for i in range(k)]

    readAndProcessCSV(filename)

    print('[+] Read %s rows' % totRows)

    outfile = outfilename(filename)
    if args.D:
        outfile = args.D + outfile
    print('[+] Writing to %s' % outfile)

    df_kl = pd.DataFrame(data=output, columns=output_col)
    print(df_kl.head())
    df_kl.to_csv(outfile)

    print('[+] Done')
