import pandas as pd
import numpy as np
import datetime
from scipy.stats import entropy

from .voting import HistogramVoter
from .filter import FlowFilter

# EXPERIMENT SETUP
filename = '../datasets/2016-06-20_SMTP_100.csv'

# EXPERIMENT VARIABLES
window = 15 # in minutes

class Experiment:
    
    # EXPERIMENT DEFAULTS
    chunksize = 100000
    self.cols = ["te", "td", "sa", "da", "sp", "dp", "pr", "flg", "fwd", "stos", "pkt", "byt", "type"]

    def __init__(self, filename, w, m, k, l):
        """[summary]

        Arguments:
            filename {string} -- file path
            w {int} -- interval length (minutes)
            m {int} -- hash function length (2 ** m)
            k {int} -- number of clones 
            l {int} -- voting parameter
        """
        # time window
        self.window_start = None
        self.window_end = None
        self.windowLength = w
        self.df_window = None
        self.df_window = pd.DataFrame(columns=self.cols)

        self.histogramVoter = HistogramVoter(w, m, k, l)

    def run(self):
        """process netflow in a bin wise fashion and output aggregated
        data
        """

        for df in pd.read_csv(self.filename, chunksize=chunksize, iterator=True):
            df.columns = cols
            df['te'] = pd.to_datetime(df['te'])
            if window_start is None:
                t = df.loc[0]['te']
                self.window_start = datetime.datetime(t.year, t.month, t.day)
                self.window_end = self.window_start + self.windowLength
            self.df_curr_window = df[(self.window_start <= df['te']) & (df['te'] < self.window_end)]
            df_next = df[self.window_end <= df['te']]
            if df_curr_window.shape[0] == 0:
                break
            while df_next.shape[0]:
                self.df_window = self.df_window.append(df_curr_window)
                self.process(self.window_start, self.df_window)
                self.df_window = pd.DataFrame(columns=cols)
                self.window_start, self.window_end = self.window_end, self.window_end + self.windowLength

                df_curr_window = df[(self.window_start <= df['te']) & (df['te'] < self.window_end)]
                df_next = df[self.window_end <= df['te']]

            self.df_window = self.df_window.append(df_curr_window)
        self.process(self.window_start, self.df_window)

        def process(self, window_start, df):
            """process current window, compute histogram distribution and then ouput meta_data by voting, then
            filter by meta_data. TODO: some analysis on the filtered flows
            
            Arguments:
                window_start {datetime} -- datetime of start of time window
                df {dataframe} -- netflows for that time window
            """
            self.histogramVoter.process_window(df)            
            df_filtered = 
