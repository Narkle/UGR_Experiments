import pandas as pd
import numpy as np
import datetime

class FlowFilter:

    cols = ['sa', 'da', 'sp', 'dp']

    def filter(self, df, meta_data):
        """filter window of flows by the meta data, keep flow as long
        as it hits one of the meta_data
        
        Arguments:
            df {dataframe} -- flow dataframe for the time window
            meta_data {[type]} -- dict: feature: list of values
        """
        def f(row):
            return any([row[k] in vals for k, vals in meta_data.items() if vals])
        return df[df.apply(lambda x: f(x), axis=1)]
        