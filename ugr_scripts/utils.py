"""helper functions for handling ugr data
"""
import pandas as pd
import numpy as np

def extract_window(df, start_time, end_time):
    """returns df whose te is in the time window
    WARNING: need to convert 'te' to datatime first 
    Arguments:
        df {dataframe} -- netflow df
        start_time {datetime}
        end_time {datetime}
    """
    return df[(start_time <= df['te']) & (df['te'] < end_time)] 


def process_csv(filename, process, w=15):
    df_curr, curr_window = None, None
    for df in pd.read_csv(filename, chunksize=10**8, iterator=True):
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
