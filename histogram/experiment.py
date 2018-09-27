"""Phase 2 of histrogram pipeline

Compute the KL divergence and filter windows when alarms are triggered

Input: Netflow csv file
Output:
    1. KL for each time window
    2. text file containing list of time windows where alarm was raised
    3. csv of filtered windows where alarm was raised and the window preceding 
       that

Usage: python experiment.py netflow.csv
"""

import argparse
import os

import pandas as pd
import numpy as np

from voting import HistogramVoter

cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']

def outfilename(filepath, tag):
    basename = os.path.basename(filepath)
    last_idx = basename.rindex('.')
    name_wo_ext = basename[:last_idx]

    return "%s.%s.csv" % (name_wo_ext, tag)


def compute_kl(filename, w, m, k, dir):
    """[summary]
    
    Arguments:
        filename {[type]} -- [description]
        w {int} -- window length (minutes)
        m {int} -- hash function length 2**m
        k {int} -- number of clones
    """

    voting = HistogramVoter(w=15,m=m,k=k,l=5) # l is any arbitrary number

    output = []
    output_col = ['time'] + ["%s_%s" % (col, i) for col in hist_cols for i in range(k)]

    def process(window_start, df):
        print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))
        row = voting.hist_kl(df)
        if row:
            output.append([window_start] + row)
    
    df_curr, curr_window = None, None
    for df in pd.read_csv(filename, chunksize=10**8, iterator=True):
        df['te'] = pd.to_datetime(df['te']).dt.floor("%dT" % w)
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

    outfile = dir + '/' + outfilename(filename, 'kl')
    print('[+] Writing to %s' % outfile)

    df_kl = pd.DataFrame(data=output, columns=output_col)
    print(df_kl.head())
    df_kl.to_csv(outfile, index=False)

    return output


def flag_events(kl):
    times = [r[0] for r in kl]
    print(times)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('k', help='number of clones', type=int)
    parser.add_argument('m', help='hash function length (2**m)', type=int)

    args = parser.parse_args()
    filename = args.f
    k = args.k
    m = args.m

    out_dir = 'exp_out'
    try:
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

    kls = compute_kl(filename, 15, m, k, out_dir)
    flag_events(kls)

    print('[+] Done')
        