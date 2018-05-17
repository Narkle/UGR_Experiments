"""
Utility functions for UGR datasets

pandas

"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from scipy.stats import entropy

import datetime
from dateutil.parser import parse
import collections
import math
import time
import os
import subprocess

import graphviz as gv
import hashlib
import pickle

#############################################
# pandas 
#############################################

def addTimebin(df, timeWidth, colName = 'te'):
    df['timebin'] = df.apply(lambda row : timebinRow(row, timeWidth, colName), axis = 1)
    return df

def timebinRow(row, timeWidth, colName):
    d = parse(row[colName])
    return int(d.timestamp() // timeWidth)

def timeBin(df, width, col = 'te'):
    """
    returns new df with new column timebin
    """
    # col = 'te'
    df['timebin'] = df.apply (lambda row: createTimebin(row, width, col),axis=1)
    return df

def timeBinToDate(x, width):
    """
    converts timebin back to date time (round down)
    """
    return datetime.datetime.fromtimestamp(x * width)

def roundDownTime(row, timeWidth, col):
    d = parse(row[col])
    epoch_seconds = int(d.timestamp() // timeWidth)
    return datetime.datetime.fromtimestamp(epoch_seconds * timeWidth)


#############################################
# Processing
#############################################

def ent(data):
    p_data= data.value_counts()/len(data) # calculates the probabilities
    H=entropy(p_data)  # input probabilities to get the entropy 
    return H

def agg(df, features):
    '''
    returns list of entropies of supplied features
    '''
    return [ent(df[feature]) for feature in features]

def aggGroups(dfGroups, timeWindow):
    features = ['sa', 'da', 'sp', 'dp' , 'flg', 'byt']
    data = [[timeBinToDate(l, timeWindow)] + agg(df, features) for l, df in dfGroups]
    columns = ['time'] + ['H({})'.format(f) for f in features]
    return pd.DataFrame(data, columns = columns)


#############################################
# Anomaly (labeled attacks)
#############################################

def timebinAnom(df, anoms):
    # warning, first column of csv has to be changed to 'te' for convenience
    df = addTimebin(df, timeWindow)
    filteredCols = ['timebin'] + anoms
    df = df[filteredCols]
    df_agg = df.groupby('timebin').agg(sum)
    df_agg['time'] = df_agg.apply(lambda row : timeBinToDate(row.name, timeWindow), axis = 1)
    filteredCols = ['time'] + anoms
    print(filteredCols)
    df_agg = df_agg.reset_index()
    df_agg = df_agg[filteredCols]
    return df_agg


#############################################
# SAM/FIM
#############################################

# SAM Stuff
def translatetoSAMinput(df, features = ['sa', 'da', 'sp', 'dp']):
    cols = list(df.columns)
    lines = []
    for index, row in df.iterrows():
        line = " ".join(["{0}:{1}".format(f, row[f]) for f in features])
        lines.append(line)
    return "\n".join(lines)

def invokeMaximalSAM(inputStr, minSup=1):
    inFileName = 'temp_binning.in'
    outFileName = 'temp_binning.out'
    
    inFile = open(inFileName, 'w+')
    inFile.write(inputStr)
    inFile.close()
    
    # maximal FIM (-m)
    subprocess.call(["./sam", '-m', '-m3', '-s' + str(minSup), inFileName, outFileName])
    lines = open(outFileName).readlines()
    
    result = {}
    for line in lines:
        *itemset, supp = line.split()
        supp = float(supp[1:-1])
        result[tuple(sorted(itemset))] = supp
    
    os.remove(inFileName)
    os.remove(outFileName)
    
    return result

def FIM(df, minSup):
    samInput = translatetoSAMinput(df)
    samOutput = invokeMaximalSAM(samInput, minSup)
    return samOutput
