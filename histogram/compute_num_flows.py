"""Compute number of flows in netflow csv
Usage: python compute_num_flows.py netflow.csv [time window]
"""

import argparse
import os

import pandas as pd
import numpy as np
from scipy.stats import entropy

def process(window_start, df):

    print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))

    vc = df['type'].value_counts()
    row = [window_start] + [vc[c] if c in vc else 0 for c in flow_types]
    output.append(row)

def readAndProcessCSV(filename, w):
    global totRows, processedRows

    df_curr, curr_window = None, None
    # erronous lines will be skipped
    for df in pd.read_csv(filename, chunksize=10**6, iterator=True, error_bad_lines=False):
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

    return "%s.counts.csv" % name_wo_ext


flow_types = ['background', 'anomaly-udpscan', 'anomaly-sshscan', 'Dos', 'scan11', 'scan44', 
           'nerisbotnet', 'anomaly-spam']
output = []
totRows = 0

if __name__ == '__main__':
    print("[+] Starting Compute Entropy")
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('--W', help='width of time window (default: 15 mins)', type=int)

    args = parser.parse_args()
    filename = args.f
    w = args.W if args.W else 15

    print("[+] Time window set to %s minutes" % w)

    readAndProcessCSV(filename, w)

    print('[+] Read %s rows' % totRows)

    outfile = outfilename(filename)
    print('[+] Writing to %s' % outfile)

    df_counts = pd.DataFrame(data=output, columns=['time'] + flow_types)
    print(df_counts.head())
    df_counts.to_csv(outfile, index=False)

    print('[+] Done')
