import pandas as pd
import numpy as np
import datetime
from scipy.stats import entropy

from hashing import HashFunction

# EXPERIMENT SETUP
filename = '../datasets/2016-06-20_SMTP_100.csv'
cols = ["te", "td", "sa", "da",	"sp", "dp",	"pr", "flg", "fwd",	"stos", "pkt", "byt", "type"]

# EXPERIMENT VARIABLES
windowLength = pd.Timedelta("15 minutes")

# PROGRAM VARIABLES
chunksize = 100000
SA_hashers = [HashFunction(seed=i) for i in range(5)]
output = []

window_start = None
window_end = None
df_window = pd.DataFrame(columns=cols)

prev_sa_hist_dist = None

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
    global processedRows
    processedRows += df.shape[0]
    process_window_df(df, window_start)
    # print('[+] Processing window %s with %s rows' % (window_start, df.shape[0]))


def process_window_df(df, window_start):
    global prev_sa_hist_dist
    curr_sa_hist_dist = sa_hist_dist(df)
    if prev_sa_hist_dist:
        kls = [kl_divergence(curr_dist, prev_dist) for curr_dist, prev_dist in zip(curr_sa_hist_dist, prev_sa_hist_dist)]
        print("[+] %s KL Divergance for sa: %s" % (window_start, kls))
    prev_sa_hist_dist = curr_sa_hist_dist


def sa_hist_dist(df):
    """distribution for each sa hashing function
    
    Arguments:
        df {dataframe} -- dataframe of the time window
    
    Returns:
        list -- list of list of distribution (of size nBins)
    """
    global SA_hashers
    hists = [{} for _ in range(len(SA_hashers))]
    for _, row in df.iterrows():
        sa = row['sa']
        for i, hasher in enumerate(SA_hashers):
            hist = hists[i]
            val = hasher.hash(sa)
            if val not in hist:
                hist[val] = 1
            else:
                hist[val] += 1
    nBins = [hasher.nBins for hasher in SA_hashers]
    return [[hist.get(b, 0) for b in range(bins)] for hist, bins in zip(hists, nBins)]

########################################################
# Stats Utility Functions
########################################################

def kl_divergence(P, Q):
    """compute kl divergance between prob dist P and Q
    
    Arguments:
        P {list} -- list of occurrance of each type
        Q {list} -- list of occurrance of each type
    """

    '''
    For prototyping only, set the occurance of each bin to 1 so get around KL
    being inf, should not be a problem when there is no sampling due to large
    input size
    '''
    P = [p if p else 1 for p in P]
    Q = [q if q else 1 for q in Q]

    return entropy(P, Q)
    

if __name__ == '__main__':
    readAndProcessCSV(filename)
    print('[+] Read %s rows' % totRows)
    print('[+] Processed %s rows' % processedRows)

