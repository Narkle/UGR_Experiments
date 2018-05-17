import sys
sys.path.append('..')

import UgrUtils as utils
import pandas as pd
import pickle
import argparse

parser = argparse.ArgumentParser(description = "time bin and aggregate by entropy")
parser.add_argument('-f', metavar='filename', type=str, help='path to csv file')
parser.add_argument('-t', metavar='timeWindow', type=int, help='width of time window')
parser.add_argument('-s', metavar='timeWindow', type=float, help='minimum support')
parser.add_argument('-o', metavar='filename', type=str, help='path to pickle')

args = parser.parse_args()
args = vars(args)

filename = args['f']
outname = args['o']
timeWindow = args['t']
minSup = args['s']

df = pd.read_csv(filename)
df = utils.addTimebin(df, timeWindow)

dict_to_pickle = utils.aggGroupsByFIM(df.groupby('timebin'), timeWindow, minSup)
pickle.dump(dict_to_pickle, open(outname, 'wb'))

print('[+] wrote to {}'.format(outname))