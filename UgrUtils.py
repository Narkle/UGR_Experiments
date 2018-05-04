"""
Utility functions for UGR datasets

pandas

"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

def createTimebin(row, timeWidth, col):
    d = parse(row[col])
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

def readAndGroupByTimebin(filename, width):
    df = pd.read_csv(filename)
    df = timeBin(df, width)
    return df.groupby('timebin')

def roundDownTime(row, timeWidth, col):
    d = parse(row[col])
    epoch_seconds = int(d.timestamp() // timeWidth)
    return datetime.datetime.fromtimestamp(epoch_seconds * timeWidth)

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
