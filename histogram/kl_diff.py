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
processedRows = 0


def process(window_start, df):
    print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))
    row = voting.hist_kl(df)
    if row:
        output.append([window_start] + row)

def readAndProcessCSV(filename):
    global totRows, processedRows, voting, window_start, df_window
    for df in pd.read_csv(filename, chunksize=200000, iterator=True):
        totRows += df.shape[0]

        df.columns = cols
        df['te'] = pd.to_datetime(df['te'])
        if window_start is None:
            t = df.loc[0]['te']
            window_start = datetime.datetime(t.year, t.month, t.day)
            window_end = window_start + windowLength
        df_curr_window = df[(window_start <= df['te']) & (df['te'] < window_end)]
        df_next = df[window_end <= df['te']]
        if df_curr_window.shape[0] == 0:
            break
        while df_next.shape[0]:
            df_window = df_window.append(df_curr_window)
            process(window_start, df_window)
            df_window = pd.DataFrame(columns=cols)
            window_start, window_end = window_end, window_end + windowLength

            df_curr_window = df[(window_start <= df['te']) & (df['te'] < window_end)]
            df_next = df[window_end <= df['te']]

        df_window = df_window.append(df_curr_window)
    process(window_start, df_window)


def outfilename(filepath):
    basename = os.path.basename(filepath)
    *name_wo_ext, _ext = basename.split('.')
    name_wo_ext = ".".join(name_wo_ext)

    return "%s.kl.csv" % name_wo_ext

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('k', help='number of clones', type=int)
    parser.add_argument('m', help='hash function length (2**m)', type=int)

    args = parser.parse_args()
    filename = args.f
    k = args.k
    m = args.m

    voting = HistogramVoter(w=15,m=m,k=k,l=5) # l is any arbitrary number
    output_col = ['time'] + ["%s_%s" % (col, i) for col in hist_cols for i in range(k)]

    readAndProcessCSV(filename)

    print('[+] Read %s rows' % totRows)
    print('[+] Processed %s rows' % processedRows)

    outfile = outfilename(filename)
    print('[+] Writing to %s' % outfile)

    df_kl = pd.DataFrame(data=output, columns=output_col)
    print(df_kl.head())
    df_kl.to_csv(outfile)

    print('[+] Done')
