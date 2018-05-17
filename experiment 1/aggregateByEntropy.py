import sys
sys.path.append('..')

import UgrUtils as utils
import pandas as pd

import argparse

parser = argparse.ArgumentParser(description = "time bin and aggregate by entropy")
parser.add_argument('-f', metavar='filename', type=str, help='path to csv file')
parser.add_argument('-t', metavar='timeWindow', type=int, help='width of time window')
parser.add_argument('-o', metavar='filename', type=str, help='path to out agg csv')

args = parser.parse_args()
args = vars(args)

filename = args['f']
outname = args['o']
timeWindow = args['t']

df = pd.read_csv(filename)
df = utils.addTimebin(df, timeWindow)


df_agg = utils.aggGroups(df.groupby('timebin'), timeWindow)
df_agg.to_csv(outname, index=False)

print('[+] wrote to {}'.format(outname))