"""
output: csv
start | sa_0_[bin#] .... | sa_1_[bin#] ....|
"""
import argparse
import os

import pandas as pd
import numpy as np
import datetime

from voting import HistogramVoter

# EXPERIMENT SETUP
filename = '../datasets/2016-06-20_SMTP_100.csv'
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
hist_cols = ['sa', 'da', 'sp', 'dp']

nClones = 5
histogramVoter = HistogramVoter(w=15, m=5, k=nClones, l=None) 
windowLength = pd.Timedelta("15 minutes")
output_col = ['time'] + ["%s_%s" % (col, i) for col in hist_cols for i in range(nClones)]
output = []
timestamps = []

chunksize = 100000

window_start = None
window_end = None
df_window = pd.DataFrame(columns=cols)

totRows = 0
processedRows = 0

def readAndProcessCSV(filename):
    global totRows, window_start, window_end, df_window
    for df in pd.read_csv(filename, chunksize=chunksize, iterator=True):
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


def process(window_start, df):
    global processedRows, histogramVoter, output, timestamps
    processedRows += df.shape[0]
    # process_window_df(df, window_start)
    print('[+] Processing window %s with %s rows' % (window_start, df.shape[0]))
    ret = histogramVoter.process_window(df)
    if ret:
        output.append([window_start] + ret)
    

def run(filename, outfilename):


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='ugr netflow csv', type=str)
    parser.add_argument('o', help='output csv', type=str)
    args = parser.parse_args()
    filename = args.f
    outfilename = args.o
    readAndProcessCSV(filename)
    print('[+] Read %s rows' % totRows)
    print('[+] Processed %s rows' % processedRows)
    # output file as csv
    df = pd.DataFrame(data=output, columns=output_col)
    df.to_csv(outfilename)