import pandas as pd
import numpy as np
import datetime
from scipy.stats import entropy

from hashing import HashFunction

THRESHOLD = 0.75

class HistogramVoter:

    hist_cols = ['sa', 'da', 'sp', 'dp']
    prev_hist_dists = {col:None for col in hist_cols}

    def __init__(self, w, m, k, l):
        """init HistogramVoter class
        
        Arguments:
            w {int} -- interval length (minutes)
            m {int} -- hash function length (2 ** m)
            k {int} -- number of clones 
            l {int} -- voting parameter
        Returns:
            [dict] -- feature -> list of feature values
        """
        
        self.w = w
        self.m = m
        self.k = k
        self.l = l

        self.feat_hashers = {col: [HashFunction(seed=i) for i in range(self.k)]
            for col in self.hist_cols}
        
        self.first_flag = True


    def process_window(self, df):
        """process window and return meta data of filters
        
        Arguments:
            df {dataframe} -- dataframe of flows for a time window
        
        Returns:
            [tuple] -- (kl_row, meta_data)
        """
        curr_hist_dists = {col:[
            [0 for i in range(2 ** self.m)] for j in range(self.k)]
            for col in self.hist_cols
        }

        for _, row in df.iterrows():
            for col in self.hist_cols:
                val = row[col]
                hashers = self.feat_hashers[col]
                hist_dists = curr_hist_dists[col]

                for hasher, hist_dist in zip(hashers, hist_dists):
                    b = hasher.hash(val)
                    hist_dist[b] += 1

        if not self.first_flag:
            meta_data = self.vote(self.prev_hist_dists, curr_hist_dists)
        else:
            self.first_flag = False
            meta_data = None
        
        self.prev_hist_dists = curr_hist_dists

        return meta_data


    def vote(self, prev_hist_dists, curr_hist_dists):
        """return meta data from voting
        
        Arguments:
            prev_hist_dists {dict} -- [description]
            curr_hist_dists {dict} -- [description]

        Returns:
            [dict] -- [feature to list of feature values]
        """
        meta_data = {}
        for col in self.hist_cols:
            prev_hists = prev_hist_dists[col]
            curr_hists = curr_hist_dists[col]
            meta_data[col] = self.vote_feature(prev_hists, curr_hists, col, THRESHOLD)

        return meta_data


    def vote_feature(self, prev_hists, curr_hists, feature, threshold):
        """get suspicious feature values by voting on histogram clones
        
        Arguments:
            prev_hists {list of list of Number} -- list of distributions
            curr_hists {list of list of Number} -- list of distributions
            feature {string} -- hist column name
            threshold {float} -- threshold to trigger value extraction
        
        Returns:
            [list] -- [feature values]
        """

        cand_feature_values = {}
        for prev, curr, hasher in zip(prev_hists, curr_hists, self.feat_hashers[feature]):
            values = self.vote_feature_clone(prev, curr, hasher, threshold)
            if values is None:
                continue
            for v in values:
                if v not in cand_feature_values:
                    cand_feature_values[v] = 1
                else:
                    cand_feature_values[v] += 1
                
        return [k for k, v in cand_feature_values.items() if v >= self.l]
    

    def vote_feature_clone(self, prev_hist, curr_hist, hasher, threshold):
        """return candidate feature values
        
        Arguments:
            prev_hist {list} -- probability distribution
            curr_hist {list} -- probability distribution
            hasher {HashFunction} -- hashing function
            threshold {float} -- threshold to start voting
        
        Returns:
            [list] -- list of feature values whose removal reduces KL to below threshold
        """

        kl = entropy(prev_hist, curr_hist)
        if kl < threshold:
            return None
        cand_bins = sorted([i for i in range(hasher.nBins)], 
            key=lambda i: abs(prev_hist[i]-curr_hist[i]), reverse=True)
        curr_idx = 0
        while kl > threshold:
            bin_to_reset = cand_bins[curr_idx]
            curr_hist[bin_to_reset] = prev_hist[bin_to_reset]
            curr_idx += 1
            kl = entropy(prev_hist, curr_hist)
        values = []
        for b in cand_bins[:curr_idx]:
            for v in hasher.mapping[b]:
                values.append(v)
        return values

        

