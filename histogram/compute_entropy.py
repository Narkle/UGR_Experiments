"""Compute Entropy for each time window
TODO: implement sliding window version
Usage: python compute_entropy.py netflow.csv
"""

import argparse
import os

import pandas as pd
import numpy as np
from scipy.stats import entropy

hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
output_col = None
output = []

df_window = pd.DataFrame(columns=cols)

totRows = 0


def process(window_start, df):
    """computes KL divergence for given df representing current time window, if first window
    return NONE
    Arguments:
        window_start {Datetime}
        df {DateFrame}
    Returns:
        False if window_start exceeds specified end time, True otherwise
    """
    if endtime and window_start >= endtime:
        print("[+] Hit end of analysis time, quitting")
        return False

    print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))
    value_counts = [df[col].value_counts() for col in hist_cols]
    row = [entropy(vc.values) for vc in value_counts]
    if row:
        output.append([window_start] + row)
    return True

def readAndProcessCSV(filename, w):
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
                to_cont = process(curr_window, df_curr)
                df_curr, curr_window = df, window
                if not to_cont:
                    break

    process(curr_window, df_curr)


def outfilename(filepath):
    basename = os.path.basename(filepath)
    last_idx = basename.rindex('.')
    name_wo_ext = basename[:last_idx]

    return "%s.entropy.csv" % name_wo_ext

if __name__ == '__main__':
    print("[+] Starting Compute Entropy")
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('--W', help='width of time window (default: 15 mins)', type=int)
    parser.add_argument('--E', help='end time of analysis', type=str)

    args = parser.parse_args()
    filename = args.f
    w = args.W if args.W else 15
    endtime = pd.to_datetime(args.E) if args.E else None

    print("[+] Time window set to %s minutes" % w)

    output_col = ['time'] + hist_cols

    readAndProcessCSV(filename, w)

    print('[+] Read %s rows' % totRows)

    outfile = outfilename(filename)
    print('[+] Writing to %s' % outfile)

    df_kl = pd.DataFrame(data=output, columns=output_col)
    print(df_kl.head())
    df_kl.to_csv(outfile)

    print('[+] Done')
