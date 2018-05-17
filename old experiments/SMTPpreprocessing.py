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

import UgrUtils as ugr
import argparse

from scipy.stats import entropy

# Experiment variables
timeWidth = 30 * 60 # 30 minutes
minSup = 1
# Program variables
chunksize = 10 ** 4

FIMdicts = {}
df_agg = pd.DataFrame(columns = ['datetime', 'flows', 'H(sa)', 'H(da)', 'H(sp)', 'H(dp)'])

parser = argparse.ArgumentParser(description = "timebin and FIM")
parser.add_argument('-f', metavar='filename', type=str, help='path to csv file')
parser.add_argument('-o1', metavar='filename', type=str, help='path to out pickle')
parser.add_argument('-o2', metavar='filename', type=str, help='path to out agg csv')
# parser.add_argument('-o', metavar='filename', type=str, help='path to out csv file')
args = parser.parse_args()
filename = vars(args)['f']

def ent(data):
    p_data= data.value_counts()/len(data) # calculates the probabilities
    H=entropy(p_data)  # input probabilities to get the entropy 
    return H

def analyse(bin, df):
	d = ugr.timeBinToDate(bin, timeWidth)
	print(d)
	dateStr = "{}-{:02}-{:02}".format(d.year, d.month, d.day)
	timeStr = "{:02}-{:02}".format(d.hour, d.minute)

	flows = len(df)
	H_sa = ent((df['sa']))
	H_da = ent((df['da']))
	H_sp = ent((df['sp']))
	H_dp = ent((df['dp']))


	df_agg.loc["{}_{}".format(dateStr, timeStr)] = [str(d), flows, H_sa, H_da, H_sp, H_dp]

	print(dateStr, timeStr)
	# df.to_csv("{}_{}.csv".format(dateStr, timeStr))

	fimoutput = ugr.FIM(df, minSup)
	# print(fimoutput)
	FIMdicts[str(d)] = fimoutput

counter = 0
df = pd.DataFrame()
currBin = 0
for chunk in pd.read_csv(filename, chunksize=chunksize):
	for row in chunk.iterrows():
		row = row[1]
		bin = ugr.createTimebin(row, timeWidth, 'te')
		if currBin == 0:
			currBin = bin

		if bin == currBin:
			df = df.append(row)
		else:
			# analysis here
			analyse(currBin, df)
			# reset df
			currBin = bin
			df = pd.DataFrame()
			df = df.append(row)
	counter += 1
	print("[+] {} chunks processed".format(counter))

outnameFIM = vars(args)['o1']
pickle.dump(FIMdicts, open(outnameFIM, 'wb'))

outnameAgg = vars(args)['o2']
df_agg.to_csv(outnameAgg, index = False)

