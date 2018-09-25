import pandas as pd
import numpy as np
import datetime
from scipy.stats import entropy

from hashing import HashFunction

THRESHOLD = 0.025

class HistogramVoter:

    hist_cols = ['sa', 'da', 'sp', 'dp', 'pkt']
    prev_hist_dists = {col:None for col in hist_cols}
    prev_kls = {col:None for col in hist_cols}


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

        self.feat_hashers = {col: [HashFunction(seed=i, length=m) for i in range(self.k)] 
            for col in self.hist_cols}
        
        self.first_flag = True

        # logging kls
        self.kl_log_cols = ["%s_%s" % (col, i) for col in self.hist_cols for i in range(k)]


    def process_window(self, df, df_kls=None, thresholds=None):
        """process window and return meta data of filters
        
        Arguments:
            df {dataframe} -- dataframe of flows for a time window
            df_kls {dataframe} -- optional dataframe to log kls
        
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
            alarm, meta_data, df_kls = self.vote(curr_hist_dists, df_kls, thresholds)
        else:
            self.first_flag = False
            alarm, meta_data = None, None
        
        self.prev_hist_dists = curr_hist_dists

        return alarm, meta_data, df_kls


    def vote(self, curr_hist_dists, df_kls=None, thresholds=None):
        """return meta data from voting
        
        Arguments:
            curr_hist_dists {dict} -- [description]
            df_kls {dataframe} -- pandas dataframe containing past kls

        Returns:
            [bool] -- whether an alarm was raised
            [dict] -- feature to list of feature values
        """
        meta_data = {}
        alarm_raised = False
        curr_kls = self.bulk_hist_dist_kl(curr_hist_dists)


        # add kls to df_kls
        if df_kls is not None:
            next_row = {}
            for col in self.hist_cols:
                kls = curr_kls[col]
                for idx, kl in enumerate(kls):
                    next_row["%s_%s" % (col, idx)] = kl
            # print(next_row)
            df_kls = df_kls.append(next_row, ignore_index=True)

        # print(curr_kls)
        # print("\t".join(["%s: %04f" % (col, sum(v)/len(v)) for col, v in curr_kls.items()]))
        # print(["%s" % col for col, v in curr_kls].join("\t"))

        for col in self.hist_cols:
            curr_hists = curr_hist_dists[col]
            feature_values = self.vote_feature(col, curr_hists, thresholds[col] if thresholds else THRESHOLD)
            if feature_values:
                alarm_raised = True
            meta_data[col] = set(feature_values)

        self.prev_kls = curr_kls        

        return alarm_raised, meta_data, df_kls

    
    def vote_feature(self, feature, curr_hists, threshold):
        """return suspicious feature values
        
        Arguments:
            prev_kls {number} -- previous kl value
            prev_hist {list} -- probability distribution
            curr_hist {list} -- probability distribution
            threshold {float} -- threshold to start voting
        
        Returns:
            [list] -- suspicious feature values
        """
        # get variables from class state
        prev_kls = self.prev_kls[feature]
        if prev_kls is None:
            return
        prev_hists = self.prev_hist_dists[feature]
        hashers = self.feat_hashers[feature]

        meta_data = {}
        for prev_kl, prev_hist, curr_hist, hasher in zip(prev_kls, prev_hists, curr_hists, hashers):
            kl = entropy(prev_hist, curr_hist)
            kl_delta = kl - prev_kl
            if kl_delta > threshold:
                kl_goal = kl - threshold
                cand_values = self.vote_feature_clone(kl_goal, kl, prev_hist, curr_hist, hasher)
                for v in cand_values:
                    if v not in meta_data:
                        meta_data[v] = 1
                    else:
                        meta_data[v] += 1
        return [k for k, v in meta_data.items() if v >= self.l]
        

    def vote_feature_clone(self, kl_goal, kl, prev_hist, curr_hist, hasher):
        cand_bins = sorted([i for i in range(hasher.nBins)], 
            key=lambda i: abs(prev_hist[i]-curr_hist[i]), reverse=True)
        curr_idx = 0
        while kl > kl_goal:
            bin_to_reset = cand_bins[curr_idx]
            curr_hist[bin_to_reset] = prev_hist[bin_to_reset]
            curr_idx += 1
            kl = entropy(prev_hist, curr_hist)
        values = []
        for b in cand_bins[:curr_idx]:
            for v in hasher.mapping[b]:
                values.append(v)
        return values


    def bulk_hist_dist_kl(self, curr_hist_dists, row_mode=False):
        """convenience method to calculate kl between two windows
        
        Arguments:
            curr_hist_dists {[type]} -- [description]
            row_mode {boolean} -- return as a flatten row (for debugging)
        
        Returns:
            [dict/list] -- if dict: col to list of kls values, else flattened list of kls
        """


        row = []
        kls = {col:[] for col in self.hist_cols}
        for col in self.hist_cols:
            prev_feat_hist = self.prev_hist_dists[col]
            curr_feat_hist = curr_hist_dists[col]
            for prev, curr in zip(prev_feat_hist, curr_feat_hist):
                if row_mode:
                    row.append(entropy(
                        [x if x else 1 for x in prev],
                        [x if x else 1 for x in curr]))
                else:
                    kls[col].append(entropy(
                        [x if x else 1 for x in prev],
                        [x if x else 1 for x in curr]))
        return row if row_mode else kls


    def hist_kl(self, df):
        """returns row for df_kl
        
        Arguments:
            df {dataframe} -- netflow dataframe
            start {datetime} -- start of time window
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
            curr_kls = self.bulk_hist_dist_kl(curr_hist_dists, row_mode=True)
        else:
            self.first_flag = False
            curr_kls = None

        self.prev_hist_dists = curr_hist_dists

        return curr_kls
