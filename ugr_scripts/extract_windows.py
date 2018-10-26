"""ad hoc script to extract windows that tripped the histogram alarm
usage: python extract_windows.py netflow.csv windows.in 15
"""

import argparse
import os

import pandas as pd
import numpy as np

cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]
df_out = pd.DataFrame(columns=cols)

def readAndProcessCSV(filename, time_windows):
    df_curr, curr_window = None, None

    def process(start, df):
        print(start)
        global df_out
        print('[+] Processing %s' % start)
        if start > time_windows[-1]:
            return True
        if start in time_windows:
            df_out = df_out.append(df)
        return False

    # erronous lines will be skipped
    for df in pd.read_csv(filename, chunksize=10**6, iterator=True, error_bad_lines=False):
        df['te'] = pd.to_datetime(df['te']).dt.floor("%dT" % w)
        grouped = df.groupby(['te'])
        break_flag = False
        for window, df in grouped:
            if df_curr is None:
                df_curr = df
                curr_window = window
                continue
            if window == curr_window:
                df_curr = df_curr.append(df)
            else:
                break_flag = process(curr_window, df_curr)
                df_curr, curr_window = df, window
                if break_flag:
                    break
        if break_flag:
            break
    process(curr_window, df_curr)

    
    outfilename = filename[:filename.rindex('.')] + '.windows.csv'
    df_out.to_csv(outfilename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('f', help='netflow csv file', type=str)
    parser.add_argument('i', help='time windows input file', type=str)
    parser.add_argument('w', help='window length to save the output', type=int)

    args = parser.parse_args()
    filename = args.f
    in_file = args.i
    w = args.w

    time_windows = [pd.to_datetime(l.strip()) for l in open(in_file).readlines()]

    print('[+] Opening %s' % filename)
    readAndProcessCSV(filename, time_windows)