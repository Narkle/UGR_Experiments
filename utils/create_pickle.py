'''
Creates pickle from netflow data
Input: csv file
OUtput: pickle of dictionary of timebins to frequent itemsets

Usage: python create_pickle.py [netflow csv]
'''

##################################################
# Paramters (Edit this before running)
##################################################

width = 5 * 60 # 5 minutes
features = ['te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg', 'fwd', 'stos', 'pkt', 'byt']

# Preamble
import argparse
import pandas as pd
import numpy as np

import datetime
from dateutil.parser import parse
import time
import os
import subprocess

import hashlib
import pickle

##################################################
# Helper Functions
##################################################

# I/O Stuff
def setIndexAsTime(df, width):
    _index = [datetime.datetime.fromtimestamp(i * width) for i in df.index]
    df.index = _index
    return df

def readAndTimeBin(filename, col, selectedFeatures, width = 60):
    df = pd.read_csv(filename)
    df['timebin'] = df.apply (lambda row: createTimebin(row, width, col),axis=1)
    df = df[['timebin'] + selectedFeatures]
    # df = setIndexAsTime(df, width)
    return df

def createTimebin(row, timeWidth, col):
    d = parse(row[col])
    return int(d.timestamp() // timeWidth)

def timeBin(df, width):
    col = 'te'
    df['timebin'] = df.apply (lambda row: createTimebin(row, width, col),axis=1)
    return df

def timeBinToDate(x, width):
    return datetime.datetime.fromtimestamp(x * width)

# SAM Stuff
def translatetoSAMinput(df, features = ['sa', 'da', 'sp', 'sp']):
    cols = list(df.columns)
    lines = []
    for index, row in df.iterrows():
        line = " ".join(["{0}:{1}".format(f, row[f]) for f in features])
        lines.append(line)
    return "\n".join(lines)

def invokeMaximalSAM(inputStr, minSup):
    inFileName = 'temp_binning.in'
    outFileName = 'temp_binning.out'
    
    inFile = open(inFileName, 'w+')
    inFile.write(inputStr)
    inFile.close()
    
    # maximal FIM (-m)
    subprocess.call(["./sam", '-m', '-m2', '-s' + str(minSup), inFileName, outFileName],
        stdout=subprocess.PIPE)
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

def pickleFIMGroup(fimGroup, sup):
    outFileName = 'output_s_{}.p'.format(str(sup).replace('.','_'))
    pickle.dump(fimGroup, open(outFileName, 'wb'))
    return outFileName

##################################################
# Main Program
##################################################

parser = argparse.ArgumentParser(description="Creates pickle of frequent itemsets from netflow data")
parser.add_argument('-f', metavar='filename',type=str,help="path to csv file")
parser.add_argument('--minSup', type=float, nargs='+')
args = parser.parse_args()


print(vars(args))
filename = vars(args)['f']
minSup = vars(args)['minSup']
# print(filename)

df = pd.read_csv(filename)
df_binned = timeBin(df, width) 
df_groups = [(timeBinToDate(i, width), x) for i, x in df_binned.groupby('timebin')]

for sup in minSup:
    FIM_groups = {l: FIM(df, sup) for l, df in df_groups}
    out = pickleFIMGroup(FIM_groups, sup)
    print("[+] Done writing to {}".format(out))


