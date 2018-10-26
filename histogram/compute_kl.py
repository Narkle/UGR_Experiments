"""Compute KL timeseries
Usage: python kl_timeseries netflow.csv
"""

import argparse
import os

import pandas as pd
import numpy as np
from scipy.stats import entropy

from hashing import HashFunction

hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
output_col = None
output = []

df_window = pd.DataFrame(columns=cols)

totRows = 0

class HistogramClones:

    hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']
    prev_hists = None

    def __init__(self, m, k):
        '''
        Arguments:
            m {int} -- hash function length (2 ** m)
            k {int} -- number of clones 
        '''

        self.m = m
        self.k = k
        self.hashers = [HashFunction(seed=i, length=m) for i in range(len(hist_cols)) 
            for j in range(self.k)]

    def value_counts_to_hists(self, value_counts):
        '''convert value counts of columns to histogram clones
        
        Arguments:
            value_counts {list of Counters}
        
        Returns:
            [type] -- [description]
        '''
        output = []
        for idx, vc in enumerate(value_counts):
            # for each columns
            hashers = self.hashers[idx*self.k:(idx+1)*self.k]
            for h in hashers:
                hist = [0 for i in range(2 ** self.m)]
                for key, count in vc.iteritems():
                    hist[h.hash(key)] += count
                output.append(hist)

        return output

    def compute_kl(self, df):
        """compute kl using previous hists as reference
        
        Arguments:
            next_hist {[type]} -- [description]

        Returns:
            list of kl values, None if first df

        Notes:
            entropy(pk, qk): compute KL Divergence as defined in literature
            pk: previous measurement interval
        """
        value_counts = [df[col].value_counts() for col in self.hist_cols]
        next_hists = self.value_counts_to_hists(value_counts)

        if self.prev_hists is None:
            self.prev_hists = next_hists
            return None

        output = []
        for prev_h, curr_h in zip(self.prev_hists, next_hists):
            for i in range(len(prev_h)):
                prev_h[i] = prev_h[i] if prev_h[i] != 0 else 1
                curr_h[i] = curr_h[i] if curr_h[i] != 0 else 1
            kl = entropy(prev_h, curr_h)
            output.append(kl)

        self.prev_hists = next_hists
        return output


def process(window_start, df, histogramClones):
    print("[+] Processing window %s with %d rows" % (window_start, df.shape[0]))
    row = histogramClones.compute_kl(df)
    if row:
        output.append([window_start] + row)


def readAndProcessCSV(filename, histogramClones, w):
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
                process(curr_window, df_curr, histogramClones)
                df_curr, curr_window = df, window
    process(curr_window, df_curr, histogramClones)


def outfilename(filepath):
    basename = os.path.basename(filepath)
    last_idx = basename.rindex('.')
    name_wo_ext = basename[:last_idx]

    return "%s.kl.csv" % name_wo_ext

if __name__ == '__main__':
    print("[+] Starting Compute KL")
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('k', help='number of clones', type=int)
    parser.add_argument('m', help='hash function length (2**m)', type=int)
    parser.add_argument('--W', help='width of time window (default: 15 mins)', type=int)
    parser.add_argument('--D', help='directory to save the output', type=str) # NOTE: not in use

    args = parser.parse_args()
    filename = args.f
    k = args.k
    m = args.m
    w = args.W if args.W else 15

    print("[+] Time window set to %s minutes" % w)

    histogramClones = HistogramClones(m, k)
    output_col = ['time'] + ["%s_%s" % (col, i) for col in hist_cols for i in range(k)]

    readAndProcessCSV(filename, histogramClones, w)

    print('[+] Read %s rows' % totRows)

    outfile = outfilename(filename)
    if args.D:
        outfile = args.D + outfile
    print('[+] Writing to %s' % outfile)

    df_kl = pd.DataFrame(data=output, columns=output_col)
    print(df_kl.head())
    df_kl.to_csv(outfile, index=False)

    print('[+] Done')
