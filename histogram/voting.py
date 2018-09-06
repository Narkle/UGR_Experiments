import pandas as pd
import numpy as np
import datetime
from scipy.stats import entropy

from hashing import HashFunction

THRESHOLD = 0.5 # some random number, definitely wrong

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

        # ignore voting for now
        # if self.prev_hist_dists:
        #     meta_data = self.vote(self.prev_hist_dists, curr_hist_dists)
        
        # prev_hist_dists = curr_hist_dists

        # return meta_data

        # for now return row of kl values for each histogram
        if self.first_flag:
            self.first_flag = False
            ret_val = None
        else:
            ret_val = self.bulk_hist_dist_kl(self.prev_hist_dists, curr_hist_dists)

        self.prev_hist_dists = curr_hist_dists
        return ret_val
    
    def bulk_hist_dist_kl(self, prev_hist_dists, curr_hist_dists):
        row = []
        for col in self.hist_cols:
            prev_feat_hist = prev_hist_dists[col]
            curr_feat_hist = curr_hist_dists[col]
            for prev, curr in zip(prev_feat_hist, curr_feat_hist):
                row.append(entropy(
                    [x if x else 1 for x in prev], 
                    [x if x else 1 for x in curr]))
        return row

    def vote_feature(self, prev_hists, curr_hists, feature, threshold):
        meta_data = set()
        for prev, curr, hasher in zip(prev_hists, curr_hists, self.feat_hashers[feature]):
            values = vote_feature_clone(prev, curr, hasher, threshold)
            meta_data.update(values)
        return list(meta_data)
    
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
        cand_bins = sorted([i for i in range(len(hasher.nBins))], key=lambda i: abs(prev_hist[i]-curr_hist[i]), reversed=True)
        curr_idx = 0
        while kl > threshold:
            bin_to_reset = cand_bins[curr_idx]
            curr_hist[bin_to_reset] = prev_hist[bin_to_reset]
            curr_idx += 1
        values = []
        for b in cand_bins[:curr_idx]:
            values.append(hasher.mapping[b])
        return values

        

