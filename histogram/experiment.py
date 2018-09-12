"""Anomaly Extraction in Netflow using Association Rules
Input: netflow as csv
Output: kl divergence across of all histogram types
"""

import argparse
import os

import pandas as pd
import numpy as np
import datetime

from voting import HistogramVoter
from filter import FlowFilter
from ruleMining import RuleMining

# EXPERIMENT SETUP
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
hist_cols = ['sa', 'da', 'sp', 'dp']
fim_cols = ['sa', 'da', 'sp', 'dp', 'np', 'nb', 'sup']

nClones = 10
minSup = 3000 # min number of flows

# Components
histogramVoter = HistogramVoter(w=15, m=5, k=nClones, l=5) 
flowFilter = FlowFilter()
ruleMining = RuleMining(minSup=minSup)

chunksize = 100000

windowLength = pd.Timedelta("15 minutes")
output_col = ['time'] + ["%s_%s" % (col, i) for col in hist_cols for i in range(nClones)]
output = []
timestamps = []

window_start = None
window_end = None
df_window = pd.DataFrame(columns=cols)


# for kl logging
kl_log_cols = ["%s_%s" % (col, i) for col in hist_cols for i in range(nClones)]
df_kl_log = pd.DataFrame(columns=kl_log_cols)

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
    global processedRows, histogramVoter, output, timestamps, df_kl_log
    processedRows += df.shape[0]

    print('[+] Processing window %s with %s rows' % (window_start, df.shape[0]))

    alarm, meta_data, df_kl_log = histogramVoter.process_window(df, df_kl_log)
    timestamps.append(window_start)

    # ignore alarm handling for now
    # if alarm:
    #     print('\t[-] Alarm raised')
    #     df_filtered = flowFilter.filter(df, meta_data)
    #     item_sets = ruleMining.mine(df_filtered)
    #     print(item_sets)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='ugr netflow csv', type=str)
    parser.add_argument('--o', help='kl output file', type=str)

    args = parser.parse_args()
    filename = args.f
    outfile = args.o

    readAndProcessCSV(filename)

    print('[+] Read %s rows' % totRows)
    print('[+] Processed %s rows' % processedRows)

    if outfile:
        df_kl_log.to_csv(outfile)
        print('[+] wrote to %s' % outfile)